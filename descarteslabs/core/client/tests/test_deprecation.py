# Copyright 2018-2024 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import unittest
import warnings

from ..deprecation import deprecate


class RequiredParametersTests(unittest.TestCase):
    @deprecate(renamed={"arg1": "arg2", "kwarg1": "kwarg2"})
    def rename(self, arg2, kwarg2=None):
        return (arg2, kwarg2)

    @deprecate(deprecated=["kwarg1"])
    def rename_deprecate(self, kwarg1=None):
        return kwarg1

    @deprecate(removed=["kwarg1"])
    def rename_deprecate_completely(self):
        return True

    @deprecate(required=["kwarg2"])
    def required(self, arg1, kwarg1=None, kwarg2=None):
        return True

    @deprecate(required=["kwarg2"], renamed={"kwarg1": "kwarg2"})
    def rename_required(self, kwarg2=None):
        return kwarg2

    @deprecate(required=["kwarg2"], renamed={"kwarg1": "kwarg2"})
    def rename_required_with_args(self, arg, kwarg2=None):
        return (arg, kwarg2)

    @deprecate(renamed={"kwarg1": "kwarg2"})
    def rename_name_clash(self, kwarg2=None):
        return True

    def test_rename(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert ("value1", "value2") == self.rename("value1", "value2")
        assert len(w) == 0

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert ("value1", "value2") == self.rename(arg1="value1", kwarg1="value2")
        assert len(w) == 2
        assert (
            "Parameter `arg1` has been renamed to `arg2`, and will "
            "be removed in future versions. Use `arg2` instead." in w[0].message.args[0]
        )
        assert (
            "Parameter `kwarg1` has been renamed to `kwarg2`, and "
            "will be removed in future versions. Use `kwarg2` instead."
            in w[1].message.args[0]
        )

    def test_required(self):
        assert self.required("value1", kwarg2="value2")
        assert self.required("value1", "value3", "value2")
        with pytest.raises(SyntaxError):
            self.required("value1")

    def test_rename_required(self):
        assert "value" == self.rename_required(kwarg2="value")
        assert "value" == self.rename_required(kwarg1="value")
        assert "value" == self.rename_required("value")

    def test_rename_required_with_args(self):
        assert ("value1", "value2") == self.rename_required_with_args(
            "value1", kwarg2="value2"
        )
        assert ("value1", "value2") == self.rename_required_with_args(
            "value1", kwarg1="value2"
        )
        assert ("value1", "value2") == self.rename_required_with_args(
            "value1", "value2"
        )
        with pytest.raises(SyntaxError):
            self.rename_required_with_args("value")

    def test_rename_name_clash(self):
        with pytest.raises(SyntaxError):
            self.rename_name_clash(kwarg1=None, kwarg2=None)

    def test_rename_deprecate(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert self.rename_deprecate(kwarg1="value") == "value"

        assert len(w) == 1
        assert (
            "Parameter `kwarg1` has been deprecated and will be removed completely "
            "in future versions." in w[0].message.args[0]
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert self.rename_deprecate("value") == "value"

        assert len(w) == 1
        assert (
            "Parameter `kwarg1` has been deprecated and will be removed completely "
            "in future versions." in w[0].message.args[0]
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.rename_deprecate_completely(kwarg1="value")

        assert len(w) == 1
        assert (
            "Parameter `kwarg1` has been deprecated and is no longer supported."
            in w[0].message.args[0]
        )

        # positional calls without a specific arg definition will fail
        # it's unknown what the user was specifying
        with pytest.raises(TypeError):
            self.rename_deprecate_completely("value")

    @deprecate(required=["arg2", "arg3"], removed=["arg1"])
    def rename_deprecate_complex(self, arg1=None, arg2=None, arg3=None):
        # assumed the original signature was (self, arg1, arg2, arg3)
        return (arg2, arg3)

    def test_rename_deprecate_complex(self):
        assert ("arg2", "arg3") == self.rename_deprecate_complex("arg1", "arg2", "arg3")


if __name__ == "__main__":
    unittest.main()
