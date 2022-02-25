"""
Tests for `jkolyer` module.
"""
import pytest
from jkolyer import jkolyer
from jkolyer.orchestration import Orchestration


class TestJkolyer(object):

    @classmethod
    def setup_class(cls):
        pass

    def test_something(self):
        pass

    @classmethod
    def teardown_class(cls):
        pass


class TestOrchestration(TestJkolyer):

    def test_create(self):
        orch = Orchestration()
        assert orch != None

