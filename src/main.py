import os
import csv
import uuid
import json

class entityBatchController(BaseController):
    def __init__(self, context: Context) -> None:
        super().__init__(context, __name__)
        self.s3_connector = S3Connector()
        self.sqs_connector = SQSConnector(context)
        self.entity_service_connector = EntityServiceConnector(context)
        self.redis_connector = RedisConnector(context)
        self.entity_batch_repository = entityBatchRepository(context)
        self.entity_request_repository = entityRequestRepository(context)
    
    
    def __create_bucket_link(self, file_key: str):
        s3_client = S3Connector.get_client()
        response = s3_client.generate_presigned_post(
            ENTITY_SERVICE_BUCKET_NAME,
            file_key,
            Fields=None,
            Conditions=None,
            ExpiresIn=LINK_EXPIRATION_TIME,
        )
        return response
        
    def __create_entity_batch(
        self,
        user: User,
        entity_batch_payload: dict,
        file_type: FileType
    ):
        description = entity_batch_payload.get("description")
        file_name = entity_batch_payload["file_name"]

        file_key = str(uuid.uuid4())        
        bucket_link = self.__create_bucket_link(file_key=file_key)

        if description is None:
            description = file_name

        entity_batch = self.entity_batch_repository.create_new_entity_batch(
            entity_batch_key=str(uuid.uuid4()),
            user=user,
            file_type=file_type,
            file_name = file_name,
            file_key=file_key,
            bucket_link = bucket_link,
            external_id=entity_batch_payload["external_id"],
            description=description,
            entity_batch_date=entity_batch_payload["entity_batch_date"]
        )

        self.entity_batch_repository.update_status(entity_batch, "pending_file_submission")
        self.entity_batch_repository.commit()
        return entity_batch
    
    def create(
        self, 
        user_key: str,
        entity_batch_payload: dict
    ):
        file_name = entity_batch_payload["file_name"]
        external_id = entity_batch_payload["external_id"]
        file_type_split = file_name.split('.')
        
        entity_batch = self.__create_entity_batch(user=get_user_by_user_key(user_key), entity_batch_payload=entity_batch_payload, file_type=file_type)
        return entity_batch
    
    def verify_uploaded_file(self, user_key: str, external_id: str):
        self.entity_batch_repository.update_status(
            entity_batch, "pending_file_validation"
        )
        self.entity_batch_repository.commit()
        self.sqs_connector.send_message(
            message_payload,
            "FILE_UPLOADED",
            SQS_QUEUE_NAME
        )
        return entity_batch
    
    def process_file(self, entity_batch_payload: dict):
        external_id = entity_batch_payload["external_id"]
        user_key = entity_batch_payload["user_key"]

        entity_batch = self.entity_batch_repository.get_entity_batch_by_external_id(external_id=external_id, user_key=user_key)

        if entity_batch is None:
            raise NotFoundentityBatch(external_id=external_id,user_key=user_key)
        
        locked_entity_batch = self.entity_batch_repository.get_and_lock_entity_batch_by_external_id(
            external_id=external_id,
            user_key=user_key,
        )
        
        if locked_entity_batch.status.enumerator != "pending_file_validation":
            raise Exception(f"Pesado{'blob'}")

        self.__validate_entity_file(locked_entity_batch=locked_entity_batch)
        self.entity_batch_repository.commit()
        return
    
    def __validate_entity_file(self, locked_entity_batch: entityBatch):
            self.sqs_connector.send_message(
                message_body=message_payload,
                message_type="CREATE_entity_BATCH",
                queue_name=SQS_QUEUE_NAME
            ) 
         
    def __verify_file_csv(self, full_path: str):
        invalid_lines = []
        valid_lines = []

        
        counter = 0

        with open(full_path, mode='r', newline='', encoding='utf-8') as csv_file:
            header = csv_file.readline()
            try:
                dialect = csv.Sniffer().sniff(header, delimiters=";,")
                csv_file.seek(0)
                csv_reader = csv.DictReader(csv_file, delimiter=dialect.delimiter)
            except Exception:
                invalid_lines.append({"error":f"malformed csv unable to read", "line": counter})
                return valid_lines, invalid_lines
            
            header_mandatory_fields = ['a', 'b', 'c', 'd', 'e']
            missing_fields = [field for field in header_mandatory_fields if field not in csv_reader.fieldnames]

            if missing_fields:
                invalid_lines.append({"error":f"missing_fields: {missing_fields}", "line": counter})
                return valid_lines, invalid_lines

            for row in csv_reader:
                counter += 1
                
                if len(row) == 0:
                    continue
                if all(not value for value in row.values()):
                    continue

                valid_lines.append(row)
        return valid_lines, invalid_lines
    
    def create_entity_requests(self, entity_batch_payload: dict):
        external_id = entity_batch_payload["external_id"]
        user_key = entity_batch_payload['user_key']
        entity_batch = self.entity_batch_repository.get_entity_batch_by_external_id(external_id, user_key)

        if entity_batch is None:
            raise NotFoundentityBatch(external_id=external_id, user_key=user_key)
        
        locked_entity_batch = self.entity_batch_repository.get_and_lock_entity_batch_by_external_id(external_id=external_id, user_key=user_key)


        file_key = locked_entity_batch.file_key
        file_type = locked_entity_batch.file_type.enumerator
        path = './'
        file_name = f'{file_key}.{file_type}'
        full_path = os.path.join(path, file_name)

        all_assets = []


        self.s3_connector.download_file(ENTITY_SERVICE_BUCKET_NAME, file_key, full_path)
        if file_type == "csv":
            all_assets = self.__process_file(full_path, locked_entity_batch)
        else:
            raise Exception('Saia fora rapaz')
        
        
        for entity_request in all_assets:
            
            message_payload = {
                "external_id": locked_entity_batch.external_id,
                "user_key": locked_entity_batch.user.user_key,
                "entity_request": {
                    "external_id": entity_request.external_id,
                    "entity_request_key": entity_request.entity_request_key,
                    }
                }
            
            self.sqs_connector.send_message(
            message_payload,
            "CREATE_entity_IN_ENTITY_SERVICE",
            SQS_QUEUE_NAME
        )
        return
        
    def __process_file(self,full_path: str, locked_entity_batch: entityBatch):
        all_assets = []
        with open(full_path, mode='r', newline='', encoding='utf-8') as csv_file:
            header = csv_file.readline()
            dialect = csv.Sniffer().sniff(header, delimiters=";,")
            csv_file.seek(0)
            csv_reader = csv.DictReader(csv_file, delimiter=dialect.delimiter)
            count = 0
            for line in csv_reader:
                count +=1
                if all(not value for value in line.values()):

                    continue
                entity_request = self.entity_request_repository.create(
                    entity_batch=locked_entity_batch,
                    external_id = str(uuid.uuid4()),
                    entity_request_key=str(uuid.uuid4())
                )
                all_assets.append(entity_request)
                

        return all_assets
                        
    def create_entity_in_entity_service(self, entity_request_payload):
        locked_entity_batch = self.entity_batch_repository.get_and_lock_entity_batch_by_external_id(
            external_id=entity_request_payload["external_id"],
            user_key=entity_request_payload["user_key"]
            )
        entity_request = self.entity_request_repository.get_by_rr_key_and_status(entity_request_payload["entity_request"]["entity_request_key"], "pending_send_to_entity_service")
        current_number_of_entities = self.redis_connector.increase_entity_batch_number_of_entities(locked_entity_batch.entity_batch_key)
        try:
            entity_request_entity_service = self.entity_service_connector.post_entity_request(
                entity_request_payload=entity_request_payload["entity_request"]
            )
            self.entity_request_repository.update_status(entity_request, "pending_user_approval")
        except BaseConnectorException as ex:
            entity_request.entity_request_denial_reason = ex.base_response.response_json
            self.entity_request_repository.update_status(entity_request, "denied")
            
        if current_number_of_entities == locked_entity_batch.number_of_entities:
            self.entity_batch_repository.update_status(locked_entity_batch, "pending_user_approval")
        self.entity_batch_repository.commit()
        return
        
    def put_user_approval(self, external_id: str, user_key: str):
        for entity_request in entity_requests:
            message_payload = {
                "external_id": entity_batch.external_id,
                "user_key": entity_batch.user.user_key,
                "entity_request": {
                    "entity_request_key": entity_request.entity_request_key,
                    }
                }

            self.sqs_connector.send_message(
            message_payload,
            "APPROVE_entity_IN_ENTITY_SERVICE",
            SQS_QUEUE_NAME
        )
        return entity_batch

    def approve_entity_in_entity_service(self, entity_request_payload):

        locked_entity_batch = self.entity_batch_repository.get_and_lock_entity_batch_by_external_id(
            external_id=entity_request_payload["external_id"],
            user_key=entity_request_payload["user_key"]
        )  
        number_of_entities = locked_entity_batch.number_of_entities

        entity_requests = self.entity_request_repository.get_and_lock_by_entity_batch_id(locked_entity_batch.id) 
        
        if locked_entity_batch.status.enumerator not in  ["on_queue_pending_entity_service_approval"]:
            raise Exception(f"entity batch status must be 'on_queue_pending_entity_service_approval', received {locked_entity_batch.status.enumerator}")
    
        self.entity_service_connector.put_user_approval(entity_request_key=entity_request_key)
        self.entity_request_repository.update_status(entity_request=entity_request, new_status="approved")
        number_of_processed_requests += 1
    
        self.entity_request_repository.update_status(entity_request=entity_request, new_status="denied")
        entity_request.entity_request_denial_reason = e.base_response.response_json
        number_of_processed_requests += 1

        