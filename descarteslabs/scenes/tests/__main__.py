import nose
import os
from nose.loader import TestLoader

os.environ['NOSE_WITH_COVERAGE'] = 'True'
os.environ['NOSE_COVER_PACKAGE'] = 'descarteslabs.scenes'

nose.main(testLoader=TestLoader(workingDir=os.path.split(os.path.abspath(__file__))[0]), argv=["", "--nocapture"])
# argv parameter allows stdout to pass through to bazel
