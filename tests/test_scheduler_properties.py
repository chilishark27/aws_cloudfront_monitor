"""
Property-Based Tests for Scheduler Lambda Handler.

This module contains property-based tests using Hypothesis to verify
the correctness properties of the scheduler_handler module.

**Validates: Requirements 1.2, 6.1, 6.3**
"""

import pytest
from hypothesis import given, strategies as st, settings

from scheduler_handler import group_accounts


class TestAccountGroupingProperties:
    """Property-based tests for account grouping functionality."""
    
    @pytest.mark.property
    @settings(max_examples=100)
    @given(
        accounts=st.lists(
            st.fixed_dictionaries({
                'Id': st.text(min_size=12, max_size=12, alphabet='0123456789'),
                'Name': st.text(min_size=1, max_size=50),
                'Email': st.emails()
            }),
            min_size=0,
            max_size=500
        ),
        group_size=st.integers(min_value=1, max_value=100)
    )
    def test_account_grouping_completeness(self, accounts, group_size):
        """
        Feature: cloudformation-deployment, Property 1: 账号分组完整性
        
        **Validates: Requirements 1.2, 6.1, 6.3**
        
        For any account list and group size, after grouping accounts:
        - The total number of accounts in all groups should equal the original list length
        - Each account should appear in exactly one group (no omissions, no duplicates)
        
        验证所有账号都被分配到某个分组，无遗漏无重复。
        """
        groups = group_accounts(accounts, group_size)
        
        # Property 1: Total accounts in all groups equals original list length
        # 所有分组中的账号总数等于原始列表长度
        total_in_groups = sum(len(g) for g in groups)
        assert total_in_groups == len(accounts), (
            f"Total accounts in groups ({total_in_groups}) does not match "
            f"original account count ({len(accounts)})"
        )
        
        # Property 2: Each account appears exactly once (no omissions, no duplicates)
        # 每个账号恰好出现一次
        all_ids = [acc['Id'] for g in groups for acc in g]
        original_ids = [acc['Id'] for acc in accounts]
        assert sorted(all_ids) == sorted(original_ids), (
            f"Account IDs in groups do not match original account IDs. "
            f"Groups have {len(all_ids)} IDs, original has {len(original_ids)} IDs"
        )
