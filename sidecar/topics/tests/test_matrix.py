import numpy as np
import pytest

from crosstalk_topics import matrix
from crosstalk_topics.errors import InvalidRequest
from crosstalk_topics.wire import Matrix


def test_round_trip_is_bit_exact() -> None:
    values = np.array([[1.0, 0.0], [-0.0, 2.5], [1e-38, 3.4e38]], dtype=np.float32)
    encoded = matrix.encode(values)
    assert (encoded.rows, encoded.columns) == (3, 2)
    decoded = matrix.decode(encoded, "m")
    assert decoded.dtype == np.float32
    assert decoded.tobytes() == values.tobytes()


def test_encoding_is_little_endian_row_major_lower_hex() -> None:
    encoded = matrix.encode(np.array([[1.0, 2.0]], dtype=np.float32))
    assert encoded.data == "0000803f00000040"


def test_empty_matrix() -> None:
    encoded = matrix.encode(np.zeros((0, 2), dtype=np.float32))
    assert encoded == Matrix(rows=0, columns=2, data="")
    assert matrix.decode(encoded, "m").shape == (0, 2)


def test_upper_case_hex_is_refused() -> None:
    with pytest.raises(InvalidRequest, match="lower-case hex"):
        matrix.decode(Matrix(rows=1, columns=1, data="0000803F"), "m")


def test_non_hex_is_refused() -> None:
    with pytest.raises(InvalidRequest, match="lower-case hex"):
        matrix.decode(Matrix(rows=1, columns=1, data="0000803g"), "m")


@pytest.mark.parametrize("data", ["0000803", "0000803f00", ""])
def test_wrong_length_is_refused(data: str) -> None:
    with pytest.raises(InvalidRequest, match="expected 8 hex digits"):
        matrix.decode(Matrix(rows=1, columns=1, data=data), "m")


@pytest.mark.parametrize("value", [np.nan, np.inf, -np.inf])
def test_non_finite_values_are_refused(value: float) -> None:
    encoded = matrix.encode(np.array([[0.5, value]], dtype=np.float32))
    with pytest.raises(InvalidRequest, match="NaN or an infinity"):
        matrix.decode(encoded, "m")


def test_encode_refuses_one_dimensional_arrays() -> None:
    with pytest.raises(ValueError, match="two-dimensional"):
        matrix.encode(np.zeros(3, dtype=np.float32))
