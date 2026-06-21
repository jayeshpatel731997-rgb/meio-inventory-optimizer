from src.optimizer import calculate_safety_stock


def test_safety_stock_uses_cv_fallback_when_std_missing():
    safety_stock = calculate_safety_stock(
        avg_demand=100,
        std_demand=0,
        lead_time=4,
        target_service_level=0.95,
    )

    assert safety_stock > 0
    assert 45 <= safety_stock <= 55
