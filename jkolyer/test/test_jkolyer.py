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

    @classmethod
    def teardown_class(cls):
        pass


class TestOrchestration(TestJkolyer):
    def test_create(self):
        self.orch = Orchestration()
        assert self.orch != None

    def test_connectDb(self):
        orch = Orchestration()
        orch.connectDb()
        assert orch.sqliteConnection is not None

    def test_disconnectDb(self):
        orch = Orchestration()
        orch.connectDb()
        orch.disconnectDb()
        assert orch.sqliteConnection is None

