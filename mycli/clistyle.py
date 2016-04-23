from pygments.token import string_to_tokentype
from pygments.style import Style
from pygments.util import ClassNotFound
from prompt_toolkit.styles import default_style_extensions, PygmentsStyle
import pygments.styles


def style_factory(name, cli_style):
    try:
        style = pygments.styles.get_style_by_name(name)
    except ClassNotFound:
        style = pygments.styles.get_style_by_name('native')

    class CLIStyle(Style):
        styles = {}

        styles.update(style.styles)
        styles.update(default_style_extensions)
        custom_styles = dict([(string_to_tokentype(x), y) for x, y in cli_style.items()])
        styles.update(custom_styles)

    return PygmentsStyle(CLIStyle)
