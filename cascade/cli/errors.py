"""A user-facing CLI error: something the user can fix (bad path, no project,
missing flag), not a bug. ``main`` prints it to stderr and exits non-zero without
a traceback."""


class CliError(Exception):
    pass