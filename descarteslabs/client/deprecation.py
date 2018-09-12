# Copyright 2018 Descartes Labs.
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

import warnings
import six


def check_deprecated_kwargs(kwargs, renames):
    """
    Warn for each key in ``kwargs`` that's been renamed.
    ``renames`` is a dict mapping {deprecated name : new name, or None if fully deprecated}
    """
    for field, renamed_to in six.iteritems(renames):
        if field in kwargs:
            if renamed_to is not None:
                msg = (
                    "The parameter `{old}` has been renamed to `{new}`."
                    "`{old}` will be removed in future versions, "
                    "please use `{new}` instead.".format(old=field, new=renamed_to)
                )
            else:
                msg = (
                    "The parameter `{}` has been deprecated "
                    "and will be removed in future versions.".format(field)
                )
            warnings.warn(msg, DeprecationWarning)
