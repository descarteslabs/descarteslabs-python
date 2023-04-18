# Copyright 2018-2023 Descartes Labs.
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


def add_bearer(token):
    """For use with Authorization headers, add "Bearer "."""
    if token:
        return ("Bearer " if isinstance(token, str) else b"Bearer ") + token
    else:
        return token


def remove_bearer(token):
    """For use with Authorization headers, strip any "Bearer "."""
    if isinstance(token, (str, bytes)) and token.lower().startswith(
        "bearer " if isinstance(token, str) else b"bearer "
    ):
        return token[7:]
    else:
        return token


def add_basic(token):
    """For use with Authorization headers, add "Basic "."""
    if token:
        return ("Basic " if isinstance(token, str) else b"Basic ") + token
    else:
        return token


def remove_basic(token):
    """For use with Authorization headers, strip any "Basic "."""
    if isinstance(token, (str, bytes)) and token.lower().startswith(
        "basic " if isinstance(token, str) else b"basic "
    ):
        return token[6:]
    else:
        return token
