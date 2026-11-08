from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "streamlit_app" / "app.py")


def test_streamlit_empty_db_shows_hint(clean_db):
    at = AppTest.from_file(APP, default_timeout=60).run()
    assert not at.exception
    assert any("No scored transactions" in i.value for i in at.info)


def test_streamlit_renders_live_data(clean_db, scorer):
    from src.common.db import insert_transactions
    from src.common.schema import add_event_id, add_event_time, canonicalize_paysim
    from src.data.generate_paysim import generate
    from src.scoring import batch_score
    insert_transactions(add_event_time(add_event_id(canonicalize_paysim(generate(300, 0.1, seed=51)))))
    batch_score.run()
    at = AppTest.from_file(APP, default_timeout=60).run()
    assert not at.exception
    assert [m.value for m in at.metric if m.label == "Transactions"] == ["300"]
    assert len(at.dataframe) == 1
