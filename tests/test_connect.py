"""
测试连接
"""
import pytest

def test_connect(loop, conn):
    """
    测试连接对象属性
    """
    assert conn.loop is loop
    assert conn.timeout == 5
    assert not conn.closed
