# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "chancy"
copyright = "2024, Tyler Kennedy"
author = "Tyler Kennedy"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.graphviz",
    "sphinx.ext.linkcode",
    "sphinx_inline_tabs",
    "sphinx_copybutton",
]

# Execute explicitly marked examples; existing bare >>> snippets may contain
# placeholders or require a running database.
doctest_test_doctest_blocks = ""

nitpicky = True
autodoc_typehints = "description"
intersphinx_mapping = {
    "python": ("https://docs.python.org/3.13", None),
    "psycopg": ("https://www.psycopg.org/psycopg3/docs", None),
}

nitpick_ignore = [
    # Generic parameters, rather than documented classes.
    ("py:obj", "chancy.job.P"),
    ("py:obj", "chancy.job.R"),
    ("py:class", "~P"),
    ("py:class", "chancy.job.R"),
    # Implementation names exposed by annotations but absent from Python's
    # inventory, which documents their public aliases instead.
    ("py:class", "_asyncio.Future"),
    ("py:class", "_asyncio.Task"),
    ("py:class", "asyncio.queues.Queue"),
    ("py:class", "asyncio.locks.Event"),
    ("py:class", "concurrent.futures._base.Future"),
    ("py:class", "multiprocessing.context.BaseContext"),
    # Starlette types have no targets in the configured inventories.
    ("py:class", "starlette.applications.Starlette"),
    ("py:class", "starlette.authentication.AuthenticationBackend"),
    ("py:class", "starlette.authentication.AuthCredentials"),
    ("py:class", "starlette.authentication.BaseUser"),
    ("py:class", "starlette.requests.Request"),
    ("py:class", "starlette.requests.HTTPConnection"),
    ("py:class", "starlette.websockets.WebSocket"),
]

copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

language = "en"

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = ["_static"]
html_logo = "../misc/logo.png"
html_title = "Chancy"

html_js_files = [
    (
        "https://cloud.umami.is/script.js",
        {
            "data-website-id": "06fdfd37-2088-44f8-885d-3a2519a2266b",
            "defer": "defer",
        },
    )
]

# -- Options for todo extension ----------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/todo.html#configuration

todo_include_todos = True


def linkcode_resolve(domain, info):
    if domain != "py":
        return None

    if not info["module"]:
        return None

    filename = info["module"].replace(".", "/")
    return f"https://github.com/tktech/chancy/blob/main/{filename}.py"
