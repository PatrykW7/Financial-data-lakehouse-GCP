from dataproc_learning import check_processing_date
import pytest 

def test_check_processing_date():
    res = check_processing_date("2026-10-05")

    assert res == "2026-10-05"



def test_invalid_check_processing_date():
    with pytest.raises(ValueError):
        check_processing_date("05-10-2026")
        