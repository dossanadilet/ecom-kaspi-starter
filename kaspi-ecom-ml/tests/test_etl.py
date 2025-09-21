from services.etl.utils import yesterday


def test_yesterday():
    y = yesterday()
    assert y is not None

