import logging
from simple_salesforce import Salesforce
from glue_common_utils.aws_utils.secret_manager import SecretManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SalesforceConnector:
    def __init__(self,credential_secret):
        self.sf = None
        self.credential_secret = credential_secret

    def connect(self):
        try:
            sm = SecretManager()
            sf_credential = sm.get_secret(self.credential_secret)
            connection_params = {
                'username': sf_credential['username'],
                'password': sf_credential['password'],
                'security_token': sf_credential.get('security_token'),
                'domain': sf_credential.get('domain')  # 'login' 或者 'test'
            }
            self.sf = Salesforce(**connection_params)
            logger.info(f"Connect Successful！API Version: {self.sf.sf_version}")
        except Exception as e:
            logger.error(e)
            raise

    def query_data(self, soql_query, paginate=True, batch_callback=None):
        """
        处理SOQL查询

        Args:
            soql_query: SOQL查询语句
            paginate: 获取全部记录还是分页获取，大数据量推荐分页获取
            batch_callback: 可选的回调函数，用于处理每个批次的数据
                           callback(batch_records, batch_num, total_batches)

        Returns:
            list: 所有记录的列表
        """
        if not self.sf:
            self.connect()

        try:
            logger.info(f"Executing SOQL Query: {soql_query[:100]}...")

            if not paginate:
                # 方法1: 使用query_all自动获取所有数据, 大数据量可能会溢出
                return self._query_all_data(soql_query)
            else:
                # 方法2: 使用query + query_more手动分页
                return self._query_with_pagination(soql_query, batch_callback)

        except Exception as e:
            logger.error(f"Query Failed: {str(e)}")
            raise

    def _query_all_data(self, soql_query):
        """使用query_all获取所有数据"""
        logger.info("Fetching all records...")

        # 执行query_all，它会自动处理所有分页
        result = self.sf.query_all(soql_query)

        # 获取总记录数
        total_size = result.get('totalSize', 0)
        logger.info(f"Total records found: {total_size}")

        # 处理记录（移除attributes字段）
        records = []
        for record in result['records']:
            record.pop('attributes', None)
            records.append(record)

        logger.info(f"Successfully retrieved {len(records)} records")
        return records

    def _query_with_pagination(self, soql_query, batch_callback=None):
        """使用分页获取所有数据"""
        logger.info("Using pagination to fetch records...")

        all_records = []
        batch_num = 0

        # 获取第一页数据
        result = self.sf.query(soql_query)
        total_size = result.get('totalSize', 0)
        logger.info(f"Total records found: {total_size}")

        while True:
            batch_num += 1
            batch_records = []

            # 处理当前批次的记录
            for record in result['records']:
                record.pop('attributes', None)
                batch_records.append(record)

            # 添加到总记录列表
            all_records.extend(batch_records)

            # 如果有回调函数，处理当前批次
            if batch_callback:
                try:
                    batch_callback(batch_records, batch_num, total_size)
                except Exception as e:
                    logger.warning(f"Batch callback failed: {e}")

            logger.info(f"Batch {batch_num}: Retrieved {len(batch_records)} records "
                        f"(Total: {len(all_records)}/{total_size})")

            # 检查是否还有更多数据
            if result.get('done', True):
                break

            # 获取下一页数据
            try:
                result = self.sf.query_more(result['nextRecordsUrl'], identifier_is_url=True)
            except Exception as e:
                logger.error(f"Failed to fetch next page: {e}")
                break

        logger.info(f"Successfully retrieved {len(all_records)} records in {batch_num} batches")
        return all_records

    def query_count(self, soql_query):
        """
        快速获取查询结果的记录数（不获取实际数据）

        Args:
            soql_query: SOQL查询语句

        Returns:
            int: 记录总数
        """
        if not self.sf:
            self.connect()

        try:
            # 修改查询为COUNT查询
            if 'SELECT' in soql_query.upper() and 'FROM' in soql_query.upper():
                # 提取FROM之后的部分
                from_index = soql_query.upper().find('FROM')
                count_query = f"SELECT COUNT() {soql_query[from_index:]}"

                # 移除可能存在的ORDER BY, LIMIT, OFFSET等
                for clause in ['ORDER BY', 'LIMIT', 'OFFSET', 'GROUP BY', 'HAVING']:
                    if clause in count_query.upper():
                        clause_index = count_query.upper().find(clause)
                        count_query = count_query[:clause_index]

                logger.info(f"Count query: {count_query}")
                result = self.sf.query(count_query)
                total_size = result.get('totalSize', 0)
                logger.info(f"Total records: {total_size}")
                return total_size
            else:
                raise ValueError("Invalid SOQL query format")

        except Exception as e:
            logger.error(f"Count query failed: {str(e)}")
            raise

    def query_in_batches(self, soql_query, batch_size=2000, batch_callback=None):
        """
        分批查询数据，适合超大数据集

        Args:
            soql_query: 基础SOQL查询（不应包含LIMIT和OFFSET）
            batch_size: 每批大小（默认2000）
            batch_callback: 回调函数 callback(batch_records, batch_num, total_batches)

        Returns:
            list: 所有记录的列表
        """
        if not self.sf:
            self.connect()

        # 首先获取总记录数
        total_records = self.query_count(soql_query)

        if total_records == 0:
            logger.info("No records found")
            return []

        all_records = []
        total_batches = (total_records + batch_size - 1) // batch_size

        logger.info(f"Fetching {total_records} records in {total_batches} batches "
                    f"(batch size: {batch_size})")

        for batch_num in range(total_batches):
            offset = batch_num * batch_size

            # 构建分页查询
            if 'ORDER BY' in soql_query.upper():
                # 如果已有ORDER BY，在它之前添加LIMIT和OFFSET
                order_by_index = soql_query.upper().find('ORDER BY')
                batch_query = (f"{soql_query[:order_by_index]} "
                               f"LIMIT {batch_size} OFFSET {offset} "
                               f"{soql_query[order_by_index:]}")
            else:
                batch_query = f"{soql_query} LIMIT {batch_size} OFFSET {offset}"

            logger.info(f"Batch {batch_num + 1}/{total_batches}: offset {offset}")

            # 执行查询
            result = self.sf.query_all(batch_query)
            batch_records = []

            for record in result['records']:
                record.pop('attributes', None)
                batch_records.append(record)

            # 添加到总记录
            all_records.extend(batch_records)

            # 执行回调
            if batch_callback:
                try:
                    batch_callback(batch_records, batch_num + 1, total_batches)
                except Exception as e:
                    logger.warning(f"Batch callback failed: {e}")

            logger.info(f"Batch {batch_num + 1} complete: {len(batch_records)} records")

            # 如果获取的记录少于batch_size，说明是最后一页
            if len(batch_records) < batch_size:
                break

        logger.info(f"All batches complete. Total records: {len(all_records)}")
        return all_records