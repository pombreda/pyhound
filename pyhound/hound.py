from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import locale
import math
import multiprocessing
import re
import sys

import requests


PY2 = sys.version[0] == '2'

DEFAULT_BATCH_SIZE = 50  # FIXME: test with a higher value?
DEFAULT_TIMEOUT = 10
N_THREADS = 10

# Warning: MATCH must have a lower value than CONTEXT
LINE_KIND_MATCH = 1
LINE_KIND_CONTEXT = 2

# See GREP_COLORS at http://www.gnu.org/software/grep/manual/html_node/Environment-Variables.html
COLOR_REPO = "\033[1m%s\033[0m"           # repo name: bold
COLOR_DELIMITER = "\033[36m%s\033[0m"     # se: cyan
COLOR_FILENAME = "\033[35m%s\033[0m"      # fn: magenta
COLOR_MATCH = "\033[1m\033[31m%s\033[0m"  # ms/mc/mt: bold red
COLOR_LINE_NUMBER = "\033[32m%s\033[0m"   # ln: green


def colorize_match(line, pattern, color):
    def colorize(re_match):
        start, end = re_match.span()
        return color % re_match.string[start:end]
    return re.subn(pattern, colorize, line)[0]


def get_lines_with_context(
        line, line_number,
        lines_before=(), lines_after=(),
        requested_before=None, requested_after=None, requested_context=None):
    """Return the given line with its line kind and its line number with
    the requested context (if any).

    This function returns an iterator. Each item is a tuple with the 3
    items: the line number, the line kind (see LINE_KIND_*) and the
    line itself.

    This implements the "-A", "-B" and "-C" options of ``grep``.
    """
    requested_before = requested_before or 0
    requested_after = requested_after or 0
    assert requested_before >= 0
    assert requested_after >= 0
    if requested_context is not None:
        assert requested_context >= 1
    if requested_before or requested_after:
        assert requested_context is None

    if requested_context is not None:
        requested_context -= 1  # Don't count the matching line.
        requested_before = int(math.ceil(requested_context / 2))
        requested_after = requested_context - requested_before
        requested_after = min(requested_after, len(lines_after))
        requested_before = requested_context - requested_after

    before = ()
    after = ()
    if requested_before and requested_before > 0:
        before = lines_before[-requested_before:]
        n_before = len(before)
    if requested_after and requested_after > 0:
        after = lines_after[:requested_after]
    for i, contextual_line in enumerate(before):
        yield line_number - n_before + i, LINE_KIND_CONTEXT, contextual_line
    yield line_number, LINE_KIND_MATCH, line
    for i, contextual_line in enumerate(after, 1):
        yield line_number + i, LINE_KIND_CONTEXT, contextual_line


def merge_lines(lines):
    """Merge lines when matching and contextual lines overlap.

    When matching and contextual lines overlap, some lines are
    duplicated. This function merges all lines so that they appear
    only once and have the right "line kind".

    FIXME: this is probably inefficient
    """
    # Sort by line number and line kind (match first).
    sorted_lines = sorted(lines, key=lambda l: (l[2], l[3]))
    seen = set()
    for repo, filename, line_number, line_kind, line in sorted_lines:
        if line_number in seen:
            continue
        seen.add(line_number)
        yield repo, filename, line_number, line_kind, line


def handle_hound_error(response):
    if 'Error' in response:
        sys.exit("Hound server returned an error: %s" % response['Error'])


def call_api(endpoint, payload=None):
    """Call API on Hound server and undecode JSON response.

    If any connection error occurs, we exit the program with an error
    message.
    """
    try:
        response = requests.get(
            endpoint,
            params=payload,
            timeout=DEFAULT_TIMEOUT,
        )
    except (requests.ConnectionError, requests.HTTPError) as exc:
        # exc.args[0] is the original exception that `requests`
        # wraps. We could probably make the output better for some
        # cases but I am not keen to guess how each exception
        # looks like.
        sys.exit("Could not connect to Hound server: %s" % exc.args[0])
    except requests.Timeout:
        sys.exit("Could not connect to Hound server: timeout.")

    try:
        json = response.json()
    except ValueError:
        sys.exit(
            "Server did not return a valid JSON response. "
            "Got this instead:\n%s" % response.text
        )
    return json


def get_search_results(endpoint, pattern, repos, path_pattern, ignore_case, rng):
    """Call Hound API to get search results."""
    payload = {
            'repos': repos,
            'rng': rng,  
            'files': path_pattern,
            'i': 'true' if ignore_case else '',
            'q': pattern,
    }
    response = call_api(endpoint, payload)
    if 'search exceeds limit' in response.get('Error', ''):
        return None
    handle_hound_error(response)
    return response['Results']


class Client(object):

    def __init__(self,
            endpoint,
            pattern,
            repos='*',
            exclude_repos=None,
            path_pattern='',
            after_context=None, before_context=None, context=None,
            color='never',
            ignore_case=False,
            show_line_number=False,
            ):
        # Endpoints
        endpoint = endpoint.rstrip('/')
        self.endpoint_list_repos = '%s/api/v1/repos' % endpoint
        self.endpoint_search = '%s/api/v1/search' % endpoint

        self.pattern = pattern

        # Hound-related options.
        self.repos = self.get_repo_list(repos, exclude_repos)
        self.path_pattern = path_pattern

        # Grep-like options.
        assert not (before_context and context)
        assert not (after_context and context)
        self.after_context = after_context
        self.before_context = before_context
        self.context = context
        if color == 'auto':
            color = sys.stdout.isatty()
        elif color == 'always':
            color = True
        else:
            color = False
        self.color = color
        self.ignore_case = ignore_case
        self.show_line_number = show_line_number

        # Internal data.
        self._matching_repos = {}
        self._left_to_retrieve = {}
        self._async_results = []
        self._results = []

    def get_repo_list(self, repos, exclude_repos):
        """Return a comma-separated list of repositories to look in.

        This method may call Hound API.
        """
        if not exclude_repos:
            return repos
        if repos == '*':
            response = call_api(self.endpoint_list_repos)
            handle_hound_error(response)
            repos = set(response.keys())
        else:
            repos = set(r.strip() for r in repos.split(','))
        exclude_repos = set(r.strip() for r in exclude_repos.split(','))
        repos = sorted(repos - exclude_repos)
        return ','.join(repos)

    def run(self):
        # FIXME: we could add a '--many' option to avoid the first "no
        # range" API call if the user thinks it is going to fail
        # because there are too many results.
        params = {
            'endpoint': self.endpoint_search,
            'pattern': self.pattern,
            'repos': self.repos,
            'path_pattern': self.path_pattern,
            'ignore_case': self.ignore_case,
        }

        results = None
        rng = ''
        while results is None:
            params['rng'] = rng
            results = get_search_results(**params)
            # Prepare range for next iteration (if needed)
            if not rng:
                rng = '0:%d' % DEFAULT_BATCH_SIZE
            else:
                end = int(rng.split(':')[1])
                end = end // 2
                if end == 0:
                    # I *think* that it would happen if a *single*
                    # file had more matches than the maximum number
                    # (5000) of matches allowed in a single request.
                    # This is defensive programming: I did not try to
                    # reproduce this behaviour.
                    sys.exit("There are too many results to retrieve in the smallest possible range.")
                rng = '%d:%d' % (0, end)

        if results is not None:
            self._results = [results]
        if rng:
            # We're here if we could not fetch all results in the
            # first request. FIXME: explain what we do.
            ranges = self._get_required_ranges(rng, results)

            pool = multiprocessing.Pool(processes=N_THREADS)
            for rng in ranges:
                p = params.copy()
                p['rng'] = rng
                # FIXME: if a query fails because there are too many
                # results, we should detect it and add two calls with
                # smaller ranges. This means that we need a wrapper
                # around get_search_results
                self._async_results.append(pool.apply_async(get_search_results, kwds=p))
            pool.close()
            pool.join()
        self.show_results()

    def _get_required_ranges(self, initial_range, results):
        max_files_with_match = 0
        for repo, result in results.items():  # FIXME: use iteritems
            n = result['FilesWithMatch']
            self._matching_repos[repo] = n
            self._left_to_retrieve[repo] = n
            max_files_with_match = max(max_files_with_match, n)

        for repo, result in results.items():  # FIXME: use iteritems
            self._left_to_retrieve[repo] -= len(result['Matches'])

        start = DEFAULT_BATCH_SIZE
        for end in range(2 * DEFAULT_BATCH_SIZE, max_files_with_match, DEFAULT_BATCH_SIZE):
            yield '%d:%d' % (start, end)
            start = end

    # FIXME: not used anymore
    # def collect_search_results(self, rng='', initial=False):
    #     """Call Hound API to perform search."""
    #     base_payload = {
    #         'repos': self.repos,
    #         'rng': rng,
    #         'files': self.path_pattern,
    #         'i': 'true' if self.ignore_case else '',
    #         'q': self.pattern,
    #     }
    #     print("request range: %s" % rng)  # FIXME: DEBUG ONLY
    #     response = call_api(self.endpoint_search, base_payload)

    #     # FIXME: this solution won't work well. Since we fill the
    #     # queue with a list of ranges to query, if one of them fail,
    #     # we need to replace it with two queries on smaller ranges,
    #     # not with a single query like it's done below.
    #     if 'search exceeds limit' in response.get('Error', ''):
    #         print("got 'too many results' error")
    #         # Too many search results with this (possibly empty) range.
    #         # Try again with a smaller range.
    #         if not rng:
    #             rng = '0:%d' % DEFAULT_BATCH_SIZE
    #         else:
    #             start, end = rng.split(':')
    #             end = (end - start) // 2
    #             if end == 0:
    #                 # I *think* that it would happen if a *single*
    #                 # file had more matches than the maximum number
    #                 # (5000) of matches allowed in a single request.
    #                 # This is defensive programming: I did not try to
    #                 # reproduce this behaviour.
    #                 sys.exit("There are too many results to retrieve in the smallest possible range.")
    #             rng = '%d:%d' % (start, end)
    #         return self.collect_search_results(rng, initial=initial)

    #     handle_hound_error(response)

    #     results = response['Results']
    #     print("Got results!")

    #     if initial:
    #         max_files_with_match = 0
    #         if not self._matching_repos:  # not initialized yet
    #             for repo, result in results.items():  # FIXME: use iteritems
    #                 n = result['FilesWithMatch']
    #                 self._matching_repos[repo] = n
    #                 self._left_to_retrieve[repo] = n
    #                 max_files_with_match = max(max_files_with_match, n)

    #     for repo, result in results.items():  # FIXME: use iteritems
    #         self._lines.extend(self.get_lines(repo, result))
    #         self._left_to_retrieve[repo] -= len(result['Matches'])

    #     if initial:
    #         # We did not get all results yet. Queue requests.
    #         if any(self._left_to_retrieve.values()):
    #             self._ranges = range(DEFAULT_BATCH_SIZE, max_files_with_match, DEFAULT_BATCH_SIZE)
    #             # for range_start in range(DEFAULT_RANGE, max_files_with_match, DEFAULT_RANGE):
    #             #     print("Putting range %d:%d in queue" % (range_start, range_start + DEFAULT_RANGE))
    #             #     self._queue.put('%d:%d' % (range_start, range_start + DEFAULT_RANGE))

    def get_lines(self, results):
        for repo, result in results.items():
            for match in result['Matches']:
                lines = []
                filename = match['Filename']
                for file_match in match['Matches']:
                    lines.extend(self.get_lines_for_repo(repo, filename, file_match))
                if self.before_context or self.after_context or self.context:
                    lines = merge_lines(lines)
                for line in lines:
                    yield line

    def get_lines_for_repo(self, repo, filename, match):
        for line_number, line_kind, line in get_lines_with_context(
                match['Line'],
                match['LineNumber'],
                match['Before'],
                match['After'],
                self.before_context,
                self.after_context,
                self.context):
            yield (repo, filename, line_number, line_kind, line)

    def show_results(self):
        # FIXME: rewrite this. We could make get_search_results return
        # a (range, results) tuple that could then be easily sorted
        # once (fore each repo) we are sure that we have all results
        # for this repo.
        lines = []
        for async_result in self._async_results:
            results = async_result.get(2)  # FIXME: fix timeout value
            lines.extend(self.get_lines(results))

        encoding = locale.getdefaultlocale()[1] or 'utf-8'
        for repo, filename, line_number, line_kind, line in lines:
            if self.show_line_number:
                fmt = "{repo}:{filename}{delim}{line_number}{delim}{line}"
            else:
                fmt = "{repo}:{filename}{delim}{line}"
            delim = ':' if line_kind == LINE_KIND_MATCH else '-'
            if self.color:
                repo = COLOR_REPO % repo
                filename = COLOR_FILENAME % filename
                line_number = COLOR_LINE_NUMBER % line_number
                delim = COLOR_DELIMITER % delim
                pattern_re = re.compile(self.pattern, flags=re.IGNORECASE if self.ignore_case else 0)
                line = colorize_match(line, pattern_re, COLOR_MATCH)
            out = fmt.format(
                repo=repo,
                filename=filename,
                line_number=line_number,
                delim=delim,
                line=line)
            # FIXME: "I'm getting heartburn. Tony, do something terrible."
            if PY2:
                print(out.encode(encoding))
            else:
                print(out)
