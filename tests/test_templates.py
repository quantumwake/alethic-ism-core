# The Alethic Instruction-Based State Machine (ISM) is a versatile framework designed to 
# efficiently process a broad spectrum of instructions. Initially conceived to prioritize
# animal welfare, it employs language-based instructions in a graph of interconnected
# processing and state transitions, to rigorously evaluate and benchmark AI models
# apropos of their implications for animal well-being. 
# 
# This foundation in ethical evaluation sets the stage for the framework's broader applications,
# including legal, medical, multi-dialogue conversational systems.
# 
# Copyright (C) 2023 Kasra Rasaee, Sankalpa Ghose, Yip Fai Tse (Alethic Research) 
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# 
# 
from alethic_ism_core.core.utils.general_utils import load_template, build_template_text


def test_load_template_relative_paths():

    template_path = './test_templates/test_template_P1_user.json'
    template = load_template(template_path)

    assert template != None

def test_template_fill():

    template_text = "hello {my_variable}, the sky is {color}, and the oceans is {thought}"

    status, built_text = build_template_text(template_text, query_state={
        "my_variable": "world",
        "color": "blue",
        "thought": "calming"
    })

    assert "hello world, the sky is blue, and the oceans is calming" == built_text
    assert status