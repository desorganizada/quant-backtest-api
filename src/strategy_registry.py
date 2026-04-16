from ee.D2Trend import D2Trend as _D2Trend
from ee.EE import EE as _EE

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return x

def _cast_trend_model_params(trend_model):
    if not trend_model or not hasattr(trend_model, 'param'):
        return
    p = trend_model.param
    if hasattr(p, 'epsilon'):
        p.epsilon = _to_float(p.epsilon)
    if hasattr(p, 'd2_ewma_bullish_threshold'):
        p.d2_ewma_bullish_threshold = _to_float(p.d2_ewma_bullish_threshold)
    if hasattr(p, 'd2_ewma_bearish_threshold'):
        p.d2_ewma_bearish_threshold = _to_float(p.d2_ewma_bearish_threshold)

class D2Trend(_D2Trend):
    def __init__(self, *args, **kwargs):
        print('>>> WRAPPED D2Trend __init__ CALLED <<<')
        super().__init__(*args, **kwargs)
        if hasattr(self, 'trend_model'):
            _cast_trend_model_params(self.trend_model)

    def on_bar_update(self, bar, t=None):
        if hasattr(self, 'trend_model'):
            _cast_trend_model_params(self.trend_model)
        return super().on_bar_update(bar, t=t)

def _cast_ee_grid_params(grid_model):
    if not grid_model or not hasattr(grid_model, 'param'):
        return
    p = grid_model.param
    if hasattr(p, 'epsilon'):
        p.epsilon = _to_float(p.epsilon)

class EE(_EE):
    def __init__(self, *args, **kwargs):
        print('>>> WRAPPED EE __init__ CALLED <<<')
        super().__init__(*args, **kwargs)
        if hasattr(self, 'grid_model'):
            _cast_ee_grid_params(self.grid_model)

    def on_bar_update(self, bar, raw_strategy_data=None, t=None):
        if hasattr(self, 'grid_model'):
            _cast_ee_grid_params(self.grid_model)
        return super().on_bar_update(bar, raw_strategy_data, t=t)
