# -*- coding: utf-8 -*-

import ipywidgets as widgets
import traitlets


class ClearableOutput(widgets.HBox):
    output = traitlets.Instance(widgets.Output)

    def __init__(self, output, on_clear=None, **kwargs):
        self._on_clear = on_clear
        self.output = output
        output.layout.flex = "1 0 auto"
        output.layout.max_width = "calc(100% - 42px)"
        output.layout.max_height = "100%"
        output.layout.overflow = "auto"

        output.observe(self._observe_output, names=["outputs"])

        clear = widgets.Button(
            description=u"✖︎",
            tooltip="Clear error logs",
            layout=widgets.Layout(width="initial", margin="5px"),
        )
        clear.on_click(self.clear)

        super(ClearableOutput, self).__init__(children=(output, clear), **kwargs)
        if len(output.outputs) == 0:
            self.layout.display = "none"

    def _observe_output(self, change):
        if len(change["new"]) == 0:
            self.layout.display = "none"
            if self._on_clear is not None:
                self._on_clear()
        else:
            self.layout.display = ""

    def clear(self, *args):
        self.output.clear_output()

    def on_clear(self, func):
        self._on_clear = func

    def append_stdout(self, output):
        self.output.append_stdout(output)

    def append_stderr(self, output):
        self.output.append_stderr(output)

    def set_output(self, output):
        self.output.outputs = output

    @property
    def outputs(self):
        return self.output.outputs
