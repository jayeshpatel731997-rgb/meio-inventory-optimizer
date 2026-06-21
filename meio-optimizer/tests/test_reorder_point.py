from src.optimizer import calculate_reorder_point


def test_reorder_point_calculation():
    reorder_point = calculate_reorder_point(avg_demand=100, lead_time=5, safety_stock=40)
    assert reorder_point == 540
