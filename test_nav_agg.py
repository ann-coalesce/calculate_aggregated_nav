import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone
import pandas as pd

from test import validate_and_enhance_balance_data, is_valid_timestamp


# ── Shared fixtures ────────────────────────────────────────────────────────────

CURR = datetime(2026, 3, 11, 10, 35, 0, tzinfo=timezone.utc)
CURR_HOUR = CURR.replace(minute=0)
PREV_HOUR = CURR_HOUR - timedelta(hours=1)

PM_MAPPING = pd.DataFrame([
    # pm,                   pm_group,   group,    fund,  active, if_btc, update_frequency
    ('pm_alpha',   'grp-alpha',   'grp', 'sp1', True,  False, 'minute'),
    ('pm_bravo',   'grp-bravo',   'grp', 'sp1', True,  False, 'minute'),
    ('pm_charlie', 'grp-charlie', 'grp', 'sp1', True,  False, 'hour'),
    ('pm_delta',   'grp-delta',   'grp', 'sp1', False, False, 'minute'),  # inactive
], columns=['pm', 'pm_group', 'group', 'fund', 'active', 'if_btc', 'update_frequency'])


def _make_balance(pm, timestamp, balance=1_000_000.0):
    return pd.DataFrame([{'timestamp': pd.Timestamp(timestamp), 'pm': pm, 'balance': balance}])


# ── is_valid_timestamp unit tests ──────────────────────────────────────────────

class TestIsValidTimestamp(unittest.TestCase):

    def test_minute_pm_with_curr_timestamp_is_valid(self):
        row = pd.Series({'update_frequency': 'minute', 'timestamp': CURR})
        self.assertTrue(is_valid_timestamp(row, CURR, CURR_HOUR))

    def test_minute_pm_with_hour_timestamp_is_invalid(self):
        """Core bug: minute PM 只拿到上一個小時的數據，應視為無效"""
        row = pd.Series({'update_frequency': 'minute', 'timestamp': CURR_HOUR})
        self.assertFalse(is_valid_timestamp(row, CURR, CURR_HOUR))

    def test_hour_pm_with_curr_hour_timestamp_is_valid(self):
        row = pd.Series({'update_frequency': 'hour', 'timestamp': CURR_HOUR})
        self.assertTrue(is_valid_timestamp(row, CURR, CURR_HOUR))

    def test_hour_pm_with_minute_timestamp_is_invalid(self):
        row = pd.Series({'update_frequency': 'hour', 'timestamp': CURR})
        self.assertFalse(is_valid_timestamp(row, CURR, CURR_HOUR))

    def test_unknown_frequency_is_treated_as_valid(self):
        row = pd.Series({'update_frequency': 'unknown', 'timestamp': PREV_HOUR})
        self.assertTrue(is_valid_timestamp(row, CURR, CURR_HOUR))


# ── validate_and_enhance_balance_data integration tests ───────────────────────

@patch('test.db_utils.get_db_table')
@patch('test.get_fallback_balance_data')
class TestValidateAndEnhance(unittest.TestCase):

    def _setup_mapping(self, mock_db):
        """讓 db_utils 的 pm_mapping 查詢回傳測試用 fixture"""
        mock_db.return_value = PM_MAPPING.copy()

    # ── 1. 正常 case ────────────────────────────────────────────────────────

    def test_all_active_pms_have_current_data(self, mock_fallback, mock_db):
        self._setup_mapping(mock_db)
        balance = pd.concat([
            _make_balance('pm_alpha',   CURR),
            _make_balance('pm_bravo',   CURR),
            _make_balance('pm_charlie', CURR_HOUR),
            _make_balance('pm_delta',   CURR),   # inactive but has data
        ])
        result, log = validate_and_enhance_balance_data(balance, CURR, CURR_HOUR)

        self.assertEqual(log['active_pms']['completely_missing'], [])
        self.assertEqual(log['active_pms']['using_fallback_data'], [])
        mock_fallback.assert_not_called()

    # ── 2. 你發現的 bug：minute PM 只有 hour 數據 ───────────────────────────

    def test_minute_pm_with_only_hour_data_triggers_fallback(self, mock_fallback, mock_db):
        """pm_alpha 是 minute PM，只有 curr_hour 的數據 → 應進 fallback，不能靜默接受"""
        self._setup_mapping(mock_db)
        balance = pd.concat([
            _make_balance('pm_alpha',   CURR_HOUR),  # ← 舊數據，應被剔除
            _make_balance('pm_bravo',   CURR),
            _make_balance('pm_charlie', CURR_HOUR),
        ])
        mock_fallback.return_value = _make_balance('pm_alpha', CURR - timedelta(minutes=1))

        # 模擬 main 裡的過濾步驟，把不符合 update_frequency 的數據剔掉再傳入
        balance = pd.merge(balance, PM_MAPPING[['pm', 'update_frequency']], on='pm', how='left')
        valid_mask = balance.apply(lambda row: is_valid_timestamp(row, CURR, CURR_HOUR), axis=1)
        balance = balance[valid_mask].drop(columns=['update_frequency'])

        result, log = validate_and_enhance_balance_data(balance, CURR, CURR_HOUR)

        fallback_pms = [f['pm'] for f in log['active_pms']['using_fallback_data']]
        self.assertIn('pm_alpha', fallback_pms)
        self.assertNotIn('pm_alpha', log['active_pms']['completely_missing'])

    # ── 3. Fallback 成功 ─────────────────────────────────────────────────────

    def test_missing_active_pm_uses_fallback_data(self, mock_fallback, mock_db):
        self._setup_mapping(mock_db)
        balance = pd.concat([
            # pm_alpha 完全沒有數據
            _make_balance('pm_bravo',   CURR),
            _make_balance('pm_charlie', CURR_HOUR),
        ])
        mock_fallback.return_value = _make_balance('pm_alpha', CURR - timedelta(minutes=2))

        result, log = validate_and_enhance_balance_data(balance, CURR, CURR_HOUR)

        fallback_pms = [f['pm'] for f in log['active_pms']['using_fallback_data']]
        self.assertIn('pm_alpha', fallback_pms)
        self.assertNotIn('pm_alpha', log['active_pms']['completely_missing'])

        # fallback row 應出現在結果裡，且 timestamp 已被更新為 curr
        alpha_rows = result[result['pm'] == 'pm_alpha']
        self.assertFalse(alpha_rows.empty)
        self.assertTrue(alpha_rows.iloc[0]['is_fallback'])
        self.assertEqual(alpha_rows.iloc[0]['timestamp'], CURR)

    # ── 4. Fallback 也沒有數據 → completely missing ──────────────────────────

    def test_missing_active_pm_with_no_fallback_is_completely_missing(self, mock_fallback, mock_db):
        self._setup_mapping(mock_db)
        balance = pd.concat([
            _make_balance('pm_bravo',   CURR),
            _make_balance('pm_charlie', CURR_HOUR),
        ])
        mock_fallback.return_value = pd.DataFrame()  # 沒有 fallback

        result, log = validate_and_enhance_balance_data(balance, CURR, CURR_HOUR)

        self.assertIn('pm_alpha', log['active_pms']['completely_missing'])
        fallback_pms = [f['pm'] for f in log['active_pms']['using_fallback_data']]
        self.assertNotIn('pm_alpha', fallback_pms)

    # ── 5. Inactive PM 沒數據 → 只 log，不 fallback ──────────────────────────

    def test_inactive_pm_missing_data_does_not_trigger_fallback(self, mock_fallback, mock_db):
        self._setup_mapping(mock_db)
        balance = pd.concat([
            _make_balance('pm_alpha',   CURR),
            _make_balance('pm_bravo',   CURR),
            _make_balance('pm_charlie', CURR_HOUR),
            # pm_delta (inactive) 沒有數據
        ])

        result, log = validate_and_enhance_balance_data(balance, CURR, CURR_HOUR)

        self.assertIn('pm_delta', log['inactive_pms']['missing_data'])
        # fallback 只能被 active PM 觸發
        called_pms = [call.args[0] for call in mock_fallback.call_args_list]
        self.assertNotIn('pm_delta', called_pms)

    # ── 6. is_fallback flag 正確傳遞 ─────────────────────────────────────────

    def test_non_fallback_rows_have_correct_flag(self, mock_fallback, mock_db):
        self._setup_mapping(mock_db)
        balance = pd.concat([
            _make_balance('pm_alpha',   CURR),
            _make_balance('pm_bravo',   CURR),
            _make_balance('pm_charlie', CURR_HOUR),
        ])
        result, _ = validate_and_enhance_balance_data(balance, CURR, CURR_HOUR)

        self.assertTrue((result[result['pm'].isin(['pm_alpha', 'pm_bravo', 'pm_charlie'])]['is_fallback'] == False).all())


if __name__ == '__main__':
    unittest.main()