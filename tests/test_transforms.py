from src.pipeline_sim.transforms import hash_id

def test_hash_id_stable():
    a = hash_id('x','y','z')
    b = hash_id('x','y','z')
    assert a == b and len(a)==16
