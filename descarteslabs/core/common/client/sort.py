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


class Sort(object):
    def __init__(self, name: str, ascending: bool):
        self.name = name
        self.ascending = ascending

    def to_string(self):
        return "{}{}".format("-" if not self.ascending else "", self.name)

    def __repr__(self) -> str:
        direction = "asc" if self.ascending else "desc"
        return f"Sort({repr(self.name)}, {repr(direction)})"
