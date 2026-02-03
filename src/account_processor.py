"""
Account Processor module for CloudFront Abuse Detection System.

This module processes AWS accounts and coordinates distribution processing.
It handles cross-account role assumption, distribution discovery, and
parallel processing of distributions within each account.
"""

import logging
from typing import Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError

from .config import Config
from .account_manager import AccountManager
from .distribution_processor import DistributionProcessor
from .observability import ObservabilityManager


logger = logging.getLogger(__name__)


class AccountProcessor:
    """
    Processes AWS accounts and coordinates distribution processing.
    
    This class handles:
    - Cross-account role assumption
    - Distribution discovery from CloudWatch
    - Parallel distribution processing using thread pools
    - Result aggregation
    - Proper exception handling for all operations
    """
    
    def __init__(
        self,
        config: Config,
        account_manager: AccountManager,
        distribution_processor: DistributionProcessor,
        observability: ObservabilityManager
    ):
        """
        Initialize Account Processor.
        
        Args:
            config: Configuration object
            account_manager: Account manager instance
            distribution_processor: Distribution processor instance
            observability: Observability manager instance
        """
        self.config = config
        self.accounts = account_manager
        self.dist_processor = distribution_processor
        self.obs = observability
    
    def process_account(self, account: Dict[str, str]) -> int:
        """
        Process a single account and return total alert count.
        
        This method:
        1. Assumes cross-account role
        2. Lists CloudFront distributions from CloudWatch
        3. Processes distributions in parallel
        4. Aggregates results
        5. Handles all exceptions gracefully
        
        Args:
            account: Account dictionary with keys:
                - Id: AWS account ID
                - Name: Account name
                - Email: Account email
                
        Returns:
            int: Total number of alerts sent for this account (0 on error)
        """
        account_id = account['Id']
        account_name = account.get('Name', 'Unknown')
        account_email = account.get('Email', 'unknown@example.com')
        
        try:
            self.obs.log_info(
                f"Processing account {account_id} ({account_name})",
                account_id=account_id,
                account_name=account_name
            )
            
            # 1. Assume cross-account role
            cw_client = self._assume_role(account_id)
            if cw_client is None:
                # Role assumption failed, record failed account
                error_msg = f"Failed to assume role in account {account_id}"
                self.accounts.record_failed_account(account_id, error_msg)
                self.obs.log_error(
                    error_msg,
                    error=Exception(error_msg),
                    account_id=account_id
                )
                return 0
            
            # 2. Get list of CloudFront distributions
            distribution_ids = self._get_distributions(cw_client)
            
            if not distribution_ids:
                self.obs.log_info(
                    f"No CloudFront distributions found in account {account_id}",
                    account_id=account_id
                )
                return 0
            
            self.obs.log_info(
                f"Found {len(distribution_ids)} distributions in account {account_id}",
                account_id=account_id,
                distribution_count=len(distribution_ids)
            )
            
            # 3. Process distributions in parallel
            total_alerts = self._process_distributions_parallel(
                account_id=account_id,
                account_name=account_name,
                account_email=account_email,
                distribution_ids=distribution_ids,
                cw_client=cw_client
            )
            
            self.obs.log_info(
                f"Completed processing account {account_id}: {total_alerts} alert(s) sent",
                account_id=account_id,
                distribution_count=len(distribution_ids),
                alerts_sent=total_alerts
            )
            
            return total_alerts
            
        except Exception as e:
            # Catch-all for unexpected errors
            self.obs.log_error(
                f"Unexpected error processing account {account_id}",
                error=e,
                account_id=account_id
            )
            # Record as failed account
            self.accounts.record_failed_account(
                account_id,
                f"Unexpected error: {str(e)[:150]}"
            )
            return 0
    
    def _assume_role(self, account_id: str):
        """
        Assume cross-account role and return CloudWatch client.
        
        This method assumes the configured role in the target account
        and returns a CloudWatch client with the assumed credentials.
        
        Args:
            account_id: AWS account ID to assume role in
            
        Returns:
            boto3.client: CloudWatch client with assumed role credentials,
                         or None if role assumption fails
        """
        try:
            # Construct role ARN
            role_arn = f"arn:aws:iam::{account_id}:role/{self.config.org_access_role}"
            
            self.obs.log_info(
                f"Assuming role in account {account_id}",
                account_id=account_id,
                role_arn=role_arn
            )
            
            # Create STS client
            sts_client = boto3.client('sts', region_name=self.config.region)
            
            # Assume role
            response = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=f"CloudFrontAbuseDetection-{account_id}",
                DurationSeconds=3600  # 1 hour
            )
            
            # Extract credentials
            credentials = response['Credentials']
            
            # Create CloudWatch client with assumed credentials
            # IMPORTANT: CloudFront metrics are ONLY available in us-east-1
            cw_client = boto3.client(
                'cloudwatch',
                region_name='us-east-1',  # CloudFront metrics are global, stored in us-east-1
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
            
            self.obs.log_info(
                f"Successfully assumed role in account {account_id}",
                account_id=account_id
            )
            
            return cw_client
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            self.obs.log_error(
                f"Failed to assume role in account {account_id}: {error_code}",
                error=e,
                account_id=account_id,
                error_code=error_code,
                error_message=error_message
            )
            
            # Return None to indicate failure
            return None
            
        except Exception as e:
            self.obs.log_error(
                f"Unexpected error assuming role in account {account_id}",
                error=e,
                account_id=account_id
            )
            return None
    
    def _get_distributions(self, cw_client) -> Set[str]:
        """
        Get list of distribution IDs from CloudWatch metrics.
        
        This method queries CloudWatch for CloudFront metrics to discover
        which distributions exist in the account. It uses the Requests metric
        as an indicator of active distributions.
        
        Args:
            cw_client: Boto3 CloudWatch client (with assumed role credentials)
            
        Returns:
            Set[str]: Set of CloudFront distribution IDs
            
        Note:
            Returns empty set if CloudWatch API call fails.
        """
        try:
            self.obs.log_info("Listing CloudFront distributions from CloudWatch metrics")
            
            # List metrics for CloudFront namespace
            # We use the Requests metric as it's present for all distributions
            paginator = cw_client.get_paginator('list_metrics')
            page_iterator = paginator.paginate(
                Namespace='AWS/CloudFront',
                MetricName='Requests'
            )
            
            distribution_ids = set()
            
            for page in page_iterator:
                for metric in page.get('Metrics', []):
                    # Extract DistributionId from dimensions
                    dimensions = metric.get('Dimensions', [])
                    for dimension in dimensions:
                        if dimension['Name'] == 'DistributionId':
                            distribution_ids.add(dimension['Value'])
            
            self.obs.log_info(
                f"Found {len(distribution_ids)} distributions in CloudWatch",
                distribution_count=len(distribution_ids)
            )
            
            return distribution_ids
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            self.obs.log_error(
                f"CloudWatch API error listing distributions: {error_code}",
                error=e,
                error_code=error_code,
                error_message=e.response['Error']['Message']
            )
            
            # Return empty set as safe default
            return set()
            
        except Exception as e:
            self.obs.log_error(
                "Unexpected error listing distributions from CloudWatch",
                error=e
            )
            return set()
    
    def _process_distributions_parallel(
        self,
        account_id: str,
        account_name: str,
        account_email: str,
        distribution_ids: Set[str],
        cw_client
    ) -> int:
        """
        Process distributions in parallel using thread pool.
        
        This method creates a thread pool and processes multiple distributions
        concurrently to improve performance. It aggregates results from all
        distribution processing tasks.
        
        Args:
            account_id: AWS account ID
            account_name: AWS account name
            account_email: AWS account email
            distribution_ids: Set of distribution IDs to process
            cw_client: Boto3 CloudWatch client (with assumed role credentials)
            
        Returns:
            int: Total number of alerts sent across all distributions
        """
        total_alerts = 0
        
        try:
            self.obs.log_info(
                f"Starting parallel processing of {len(distribution_ids)} distributions",
                account_id=account_id,
                distribution_count=len(distribution_ids),
                max_workers=self.config.dist_max_workers
            )
            
            # Create thread pool for parallel distribution processing
            with ThreadPoolExecutor(max_workers=self.config.dist_max_workers) as executor:
                # Submit all distribution processing tasks
                future_to_dist = {
                    executor.submit(
                        self.dist_processor.process_distribution,
                        account_id,
                        account_name,
                        account_email,
                        dist_id,
                        cw_client
                    ): dist_id
                    for dist_id in distribution_ids
                }
                
                # Process results as they complete
                for future in as_completed(future_to_dist):
                    dist_id = future_to_dist[future]
                    
                    try:
                        # Get result from completed task
                        alerts_sent = future.result()
                        total_alerts += alerts_sent
                        
                        if alerts_sent > 0:
                            self.obs.log_info(
                                f"Distribution {dist_id} sent {alerts_sent} alert(s)",
                                account_id=account_id,
                                distribution_id=dist_id,
                                alerts_sent=alerts_sent
                            )
                        
                    except Exception as e:
                        # Task raised an exception
                        self.obs.log_error(
                            f"Distribution processing task failed for {dist_id}",
                            error=e,
                            account_id=account_id,
                            distribution_id=dist_id
                        )
                        # Continue processing other distributions
                        continue
            
            self.obs.log_info(
                f"Completed parallel processing: {total_alerts} total alert(s)",
                account_id=account_id,
                distribution_count=len(distribution_ids),
                total_alerts=total_alerts
            )
            
            return total_alerts
            
        except Exception as e:
            self.obs.log_error(
                "Error in parallel distribution processing",
                error=e,
                account_id=account_id
            )
            # Return whatever alerts we managed to send
            return total_alerts
