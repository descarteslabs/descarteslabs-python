## Compilation of generated code
After modifying the proto files no additional steps need to be taken to support the dependent golang targets. 

For python you must run the following tool to copy the generated python files from the bazel cache, and then copy them 
to the right place in the repository.

```
$ bazel run //tools/copy_python_gen_code
```

## GO IDE support
To copy the generated go files out of the bazel cache, to support IDE integration run the following target:
```
$ bazel run //tools/copy_go_gen_code
```

Note: The generated go files should *not* be checked in. 

## Todo (winston):
get python generated code completly into bazel, and remove it from git. This requires also solving client packaging and integrating with copybara.