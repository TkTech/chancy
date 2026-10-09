import abc
import importlib
import inspect
import warnings
from collections.abc import Callable, Iterable
from typing import Any, TypeAlias

#: A value in its JSON form: what :meth:`Validator.dump` returns and what
#: :meth:`Validator.load` receives.
JSON: TypeAlias = (
    dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None
)


class JobValidationError(Exception):
    """
    Raised when a job's kwargs are rejected by a validator, or its function
    exists but fails to import, when the job is pushed or when it runs. The
    original error is its ``__cause__``.
    """

    def __init__(self, func: str, parameter: str | None, reason: str):
        # Every argument is passed on, so the error survives pickling when a
        # job fails in a child process.
        super().__init__(func, parameter, reason)
        #: The importable name of the job's function.
        self.func = func
        #: The parameter whose kwarg was rejected, or None when the function
        #: itself couldn't be imported.
        self.parameter = parameter
        #: The original error's message.
        self.reason = reason

    def __str__(self) -> str:
        if self.parameter is None:
            return f"Could not validate job {self.func!r}: {self.reason}"
        return (
            f"Invalid kwarg {self.parameter!r} for job {self.func!r}:"
            f" {self.reason}"
        )


class ValidationSkippedWarning(RuntimeWarning):
    """
    Emitted when a job is pushed from a process where its function can't be
    found. The job is pushed anyway and validated when it runs.
    """


class Validator(abc.ABC):
    """
    Validates job kwargs against the annotations of their function's
    parameters, connecting a validation library to Chancy. Configure
    validators with the ``validators`` argument of
    :class:`~chancy.app.Chancy`; a parameter is handled by the first one
    whose :meth:`supports` accepts its annotation.

    :meth:`dump` runs when a job is pushed and :meth:`load` when it runs.
    With the process and sub-interpreter executors, validators are pickled to
    where the job runs. See :doc:`/howto/jobs` for examples.
    """

    @abc.abstractmethod
    def supports(self, annotation: object) -> bool:
        """
        Return whether this validator validates parameters with this
        annotation.
        """

    @abc.abstractmethod
    def load(self, annotation: Any, value: JSON) -> object:
        """
        Turn the stored JSON into the object passed to the function,
        validating it. Raise the library's own error for invalid data.
        """

    @abc.abstractmethod
    def dump(self, annotation: Any, value: Any) -> JSON:
        """
        Turn the pushed object into the JSON stored in the queue. Raise if the
        value isn't an object this validator handles.
        """


def dump_kwargs(
    validators: Iterable[Validator], func: str, kwargs: dict[str, Any]
) -> dict[str, Any]:
    """
    Dump the kwargs of a job whose annotation a validator supports. Chancy
    calls this when a job is pushed.

    :param validators: The validators to use, in the order they are tried.
    :param func: The importable name of the job's function.
    :param kwargs: The job's kwargs.
    :raises JobValidationError: If a validator rejects a kwarg, or the
        function can't be imported, with the original error as its cause.
    :return: The kwargs to store.
    """
    validators = list(validators)
    if not validators:
        return dict(kwargs)

    function = _find_function(func)
    if function is None:
        return dict(kwargs)

    return _convert_kwargs(
        validators,
        func,
        function,
        kwargs,
        lambda v, annotation, value: v.dump(annotation, value),
    )


def load_kwargs(
    validators: Iterable[Validator],
    func: str,
    function: Callable[..., object],
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """
    Load the kwargs of a job whose annotation a validator supports. Chancy
    calls this when a job runs.

    :param validators: The validators to use, in the order they are tried.
    :param func: The importable name of the job's function.
    :param function: The job's function, as the executor resolved it.
    :param kwargs: The kwargs the function is about to be called with, the
        annotated ones in their stored JSON form.
    :raises JobValidationError: If a validator rejects a kwarg, with the
        validator's error as its cause.
    :return: The kwargs to call the function with.
    """
    return _convert_kwargs(
        list(validators),
        func,
        function,
        kwargs,
        lambda v, annotation, value: v.load(annotation, value),
    )


def _convert_kwargs(
    validators: list[Validator],
    func: str,
    function: Callable[..., object],
    kwargs: dict[str, Any],
    convert: Callable[[Validator, Any, Any], Any],
) -> dict[str, Any]:
    kwargs = dict(kwargs)
    if not validators:
        return kwargs

    for name, param in inspect.signature(function).parameters.items():
        if name not in kwargs or param.annotation is param.empty:
            continue

        validator = next(
            (v for v in validators if v.supports(param.annotation)), None
        )
        if validator is None:
            continue

        try:
            kwargs[name] = convert(validator, param.annotation, kwargs[name])
        # Validators raise their library's own errors for invalid data.
        except Exception as exc:
            raise JobValidationError(func, name, str(exc)) from exc
    return kwargs


def _find_function(func: str) -> Callable[..., object] | None:
    """
    Import a job's function by its name, or return None with a warning when
    it can't be found in this process. Any other import error is raised as a
    :class:`JobValidationError`.
    """
    mod_name, _, func_name = func.rpartition(".")
    module = None
    try:
        if mod_name:
            module = importlib.import_module(mod_name)
    except ModuleNotFoundError as exc:
        # A module missing inside the job's own module is a real error.
        if exc.name is None or not (
            mod_name == exc.name or mod_name.startswith(f"{exc.name}.")
        ):
            raise JobValidationError(
                func, None, f"could not import its function: {exc}"
            ) from exc
    # Importing runs the module's code, which may raise anything.
    except Exception as exc:
        raise JobValidationError(
            func, None, f"could not import its function: {exc}"
        ) from exc

    function = getattr(module, func_name, None)
    if function is None:
        warnings.warn(
            f"Could not find {func!r} to validate the kwargs of its job. It"
            " will be validated when it runs.",
            ValidationSkippedWarning,
        )
    return function
