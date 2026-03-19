from contextlib import contextmanager
from dataclasses import fields, is_dataclass
from os import getenv
from yaml import safe_load
from typing import TypeVar

T = TypeVar("T")


def Configuration(environment_variable: str):
    class ConfigurationBaseMeta(type):
        def __call__(self, *args, **kwargs):
            base_configuration = dict()
            if (configuration_file := getenv(environment_variable)) is not None:
                with open(configuration_file) as file:
                    file_configuration = safe_load(file)
                base_configuration.update(file_configuration)
            base_configuration.update(kwargs)
            return super().__call__(*args, **base_configuration)

    current_configuration = None

    class ConfigurationBase(metaclass=ConfigurationBaseMeta):
        def __post_init__(self):
            def traverse_init(object_, type_):
                for field in fields(type_):
                    attribute = getattr(object_, field.name)
                    if type(attribute) is dict and is_dataclass(field.type):
                        setattr(object_, field.name, field.type(**attribute))
                        traverse_init(getattr(object_, field.name), field.type)

            traverse_init(self, type(self))

        @classmethod
        @contextmanager
        def load(class_, configuration_file: str | None):
            if configuration_file is None:
                file_configuration = {}
            else:
                with open(configuration_file) as file:
                    file_configuration = safe_load(file)
            with class_.override(**file_configuration):
                yield

        @classmethod
        @contextmanager
        def override(class_, **kwargs):
            nonlocal current_configuration
            current_configuration = class_(**kwargs)
            yield
            current_configuration = None

        @classmethod
        def get(class_: T) -> T:
            nonlocal current_configuration
            return current_configuration

    return ConfigurationBase
