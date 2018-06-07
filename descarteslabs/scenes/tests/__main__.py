import nose
import os
from nose.loader import TestLoader

nose.main(testLoader=TestLoader(workingDir=os.path.split(os.path.abspath(__file__))[0]), argv=["", "--nocapture"])
# argv parameter allows stdout to pass through to bazel
