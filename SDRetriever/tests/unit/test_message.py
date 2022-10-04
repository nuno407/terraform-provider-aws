
import json
from datetime import datetime
import pytest
from sdretriever.message import Chunk, Message, VideoMessage, SnapshotMessage

sqs_message_queue_selector =  {'MessageId': '8e8d79bb-8a6b-4461-a94d-51c7aa9bb0f1', 'ReceiptHandle': 'AQEBtCBwuG54U0wqc572u5hzyBPmxsd7I+cOT7zAsKG4foVAvgYTG5UDz3DZ07IG6t61OKtInqBeOGpLA0AhQOPdHum2hRXA8RHwu7iznWK6wll/mZGKtLIyPHUG5NflRllxCY698TeVxyqvGgKqWqTQipV1XOTGpxw4X+5L1ScIsTdlLrj1KAGl61h5s+ZEOOvoQiMdP2ecYshf9LSul80BLdNe8l5iAsg/JJwODYhv/hubXSAKjDrLuDxjPQNq8PaCP+WC5h0iWeP3WsPfq1uXvulkPxdm8PGADJJuBw9vwSoHqociFlDP08hYZDqzi5EMDe8+PCoC26l4V9oQ/cWq8k7Peh2nsBulp2f6NIWfen9/5YDkkNVrwMy8MXmmUg7YNPVboGjgeVAfrExighxI+o4/KqgqonU1Qznd44r0nYU=', 'MD5OfBody': 'cfe61891e60d51ca14d662c3cfed3a76', 'Body': '{\n  "Type" : "Notification",\n  "MessageId" : "a24584ea-fe55-56a9-bd2f-dfada4266b2c",\n  "TopicArn" : "arn:aws:sns:eu-central-1:736745337734:prod-inputEventsTerraform",\n  "Message" : "{\\"topic\\":\\"com.bosch.rcc/srx_herbie_test_sim_am_01/things/twin/events/modified\\",\\"headers\\":{\\"orig_adapter\\":\\"hono-mqtt\\",\\"qos\\":\\"1\\",\\"device_id\\":\\"com.bosch.rcc:srx_herbie_test_sim_am_01\\",\\"creation-time\\":\\"1657624627540\\",\\"traceparent\\":\\"00-0a84e899503b765b0ca9c96d86390022-98345f5e303cee11-00\\",\\"kafka.timestamp\\":\\"1657624627540\\",\\"kafka.topic\\":\\"hono.telemetry.tbccf1729c0ac4f748ef8c4a62a953076_hub\\",\\"orig_address\\":\\"telemetry\\",\\"kafka.key\\":\\"com.bosch.rcc:srx_herbie_test_sim_am_01\\",\\"ditto-originator\\":\\"integration:bccf1729-c0ac-4f74-8ef8-c4a62a953076_things:hub\\",\\"response-required\\":false,\\"version\\":2,\\"requested-acks\\":[],\\"content-type\\":\\"application/json\\",\\"correlation-id\\":\\"2c17e5c9-e687-42a4-914e-1fdac9d42af3\\"},\\"path\\":\\"/features/com.bosch.ivs.videorecorder.UploadRecordingEvent\\",\\"value\\":{\\"properties\\":{\\"header\\":{\\"message_type\\":\\"com.bosch.ivs.videorecorder.UploadRecordingEvent\\",\\"timestamp_ms\\":1657624627537,\\"message_id\\":\\"de13f5cc-dfea-46da-8d89-9e3884e30a24\\",\\"device_id\\":\\"srx_herbie_test_sim_am_01\\",\\"boot_id\\":\\"d20ba602-2c06-4405-8ed5-db941573dbbe\\"},\\"correlation_id\\":\\"bda19c79-eac4-4d87-af31-632208257ab3\\",\\"recording_id\\":\\"TrainingMultiSnapshot-bc4d26db-9689-4d77-b173-a21fb26776fa\\",\\"recorder_name\\":\\"TrainingMultiSnapshot\\",\\"command_status\\":{\\"status_code\\":\\"COMMAND_STATUS_CODE__OK\\",\\"details\\":\\"COMMAND_STATUS_CODE__OK\\"}}},\\"extra\\":{\\"attributes\\":{\\"vin\\":\\"unknown\\",\\"tenant\\":\\"TEST_TENANT\\",\\"vehicleType\\":\\"CARMODEL__FIAT_500_312\\",\\"subjectId\\":\\"15afc9be-5e9f-4ca0-b5bd-795420876b09\\",\\"operationMode\\":\\"SAFETY_CALL\\",\\"deviceType\\":\\"hailysharey\\"}},\\"revision\\":4206,\\"timestamp\\":\\"2022-07-12T11:17:07.551341075Z\\"}",\n  "Timestamp" : "2022-07-12T11:17:07.585Z",\n  "SignatureVersion" : "1",\n  "Signature" : "p8ZAMYlyEkbAG+Bg31+ZmUVvw4LN7FC40JoGmGD1Cye7qJh4dXLIuptICLYvMgcYjeb5SVEavW7DWm7dKALGYuRnJH5ffy4Kwd7P6P63IOD1tLHvYMpGSxwSJX4gvgBW6juqkd3lqLrwOVQ21PgGVtdFI/xD8BK7UhDutQZKtWhDXVAWpV1Bqls0m6edOoGu+yhfBwOGG7Kv+AYXQ63xgKseZnsLCRMU5KU4la10Ik/zjk6HSgsI2iWE5hEK1nMWH+Lrb4gxfLjnOGJaUyTxUc6c5uapWPnytc9bKz9/ARqaXgV146nzzr86vjlk26bRayLX9Q5H6mm/wTmQPg1uDw==",\n  "SigningCertURL" : "https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",\n  "UnsubscribeURL" : "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:736745337734:prod-inputEventsTerraform:23a2a7a5-1fd3-4bbe-b766-1da13b0577d9",\n  "MessageAttributes" : {\n    "deviceType" : {"Type":"String","Value":"hailysharey"},\n    "operationMode" : {"Type":"String","Value":"SAFETY_CALL"},\n    "eventType" : {"Type":"String","Value":"com.bosch.ivs.videorecorder.UploadRecordingEvent"},\n    "tenant" : {"Type":"String","Value":"TEST_TENANT"},\n    "subjectId" : {"Type":"String","Value":"15afc9be-5e9f-4ca0-b5bd-795420876b09"}\n  }\n}', 'Attributes': {'SentTimestamp': '1657624627631'}}
sqs_message_queue_download = {'MessageId': '6615420b-68df-4b65-94e3-8e4ed63935f4', 'ReceiptHandle': 'AQEBhAkFOkQgJridT/VXqegxOFUc9D8VpMaclfNDlkOw1GJ78ocsIWRujqkYFSm1hfc11vC9i9Yjjspw6Qnq4qfpv1DWCX17ciEz1N5wZDmUa6o56RlHUwSBJLcnY3URXmEaXMM3rGnUkYV0x6G9GWOw4oV+BOnFH6TnUvDS/UBlDax/zGPqPOz6kqYkHP8MS7fn4FuwTr9LuBr4m/bxUcThgmyNaBk7piDgpChbd9PZ2G6zmt3axqYoaGummttEzabOrt1I+IWxO+Yst5MufZiY5/DZlIwj3Jng/ubXT4HY038S9JGZSXKjFMM3L2wkQ4MEjwANOXKJtv8/x/HbBhcqUoCuOdhee/7gKHpczMUNkHZ1rQAgY0l1UzxQt1OG6AMe7Os5SBhQ9g2O5LNgJ+cHgAVDJwbtV7xPFwN6Q3NeFIQ=', 'MD5OfBody': 'c62f87681f2317a323c99a05dd13ef63', 'Body': '{\n  "Type" : "Notification",\n  "MessageId" : "d172193a-1f8c-5487-9867-427844fa2dce",\n  "TopicArn" : "arn:aws:sns:eu-central-1:213279581081:dev-video-footage-events",\n  "Message" : "{\\"streamName\\":\\"datanauts_DATANAUTS_DEV_01_TrainingRecorder\\",\\"footageFrom\\":1657297040802,\\"footageTo\\":1657297074110,\\"uploadStarted\\":1657297192968,\\"uploadFinished\\":1657297196322}",\n  "Timestamp" : "2022-07-08T16:20:06.865Z",\n  "SignatureVersion" : "1",\n  "Signature" : "nAsyjqO90ZlZr/SF99sqeDy5wU8zbduZrPiSN+KeG88lzpEvIrytXie2TdC0LHqoKtp2FAKdO91bXyaSJthQLO/hmlnTX5GMG7H2wbnBDuELt2hQNdbU0Y1Jjg6d8DFcxVAVfV0KoUM6uBAiQdMYu+AxqshDL16/sB44TLcSujx3zXAdaS/6bPmayFu7ojWTQWZ0cUQQajzgRCb1OvT73utLSNQaAFh+2F3WPIk6gM1nyoKdii5X53CduO2GWjjX6sF9CqHANA+mJZ4dcD6KyG0SUrOqD/q4PRCVsyTNUd5oJ5+J9XfNnGZ4L1QgtTX36zkFsGjYQhVWiYVs6tVoRg==",\n  "SigningCertURL" : "https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",\n  "UnsubscribeURL" : "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:213279581081:dev-video-footage-events:e6853099-4723-45cc-b9ed-751484360ce8",\n  "MessageAttributes" : {\n    "recorder" : {"Type":"String","Value":"TRAINING"},\n    "recordingId" : {"Type":"String","Value":"TrainingRecorder-b8f0525f-706c-431a-a93b-0e9a8b66fd6c"},\n    "deviceId" : {"Type":"String","Value":"DATANAUTS_DEV_01"},\n    "tenant" : {"Type":"String","Value":"datanauts"}\n  }\n}', 'Attributes': {'SentTimestamp': '1657297206893'}}

@pytest.mark.unit
class TestChunk:
    fields = ["uuid", "upload_status", "start_timestamp_ms", "end_timestamp_ms", "payload_size"]

    def test_empty_input(self):
        chunk_description = dict()
        obj = Chunk(chunk_description)
        for field in self.fields:
            assert not eval(f"obj.{field}")

    def test_valid_input(self):
        chunk_description = dict(uuid=1, upload_status="status", start_timestamp_ms=123,end_timestamp_ms=321, payload_size=1000)
        obj = Chunk(chunk_description)
        for field in self.fields:
            assert eval(f"obj.{field}") == chunk_description[f"{field}"]

@pytest.mark.unit
class TestMessage:
    fields = ["messageid", "receipthandle", "attributes", "timestamp", "messageattributes", "message", "tenant", "topicarn", "deviceid", "body"]

    def test_empty_input(self):
        sqs_message = dict()
        obj = Message(sqs_message)
        for field in self.fields:
            assert not eval(f"obj.{field}")
    
    def test_valid_input(self):
        expected = {
            'messageid':'6615420b-68df-4b65-94e3-8e4ed63935f4',
            'receipthandle':'AQEBhAkFOkQgJridT/VXqegxOFUc9D8VpMaclfNDlkOw1GJ78ocsIWRujqkYFSm1hfc11vC9i9Yjjspw6Qnq4qfpv1DWCX17ciEz1N5wZDmUa6o56RlHUwSBJLcnY3URXmEaXMM3rGnUkYV0x6G9GWOw4oV+BOnFH6TnUvDS/UBlDax/zGPqPOz6kqYkHP8MS7fn4FuwTr9LuBr4m/bxUcThgmyNaBk7piDgpChbd9PZ2G6zmt3axqYoaGummttEzabOrt1I+IWxO+Yst5MufZiY5/DZlIwj3Jng/ubXT4HY038S9JGZSXKjFMM3L2wkQ4MEjwANOXKJtv8/x/HbBhcqUoCuOdhee/7gKHpczMUNkHZ1rQAgY0l1UzxQt1OG6AMe7Os5SBhQ9g2O5LNgJ+cHgAVDJwbtV7xPFwN6Q3NeFIQ=',
            'attributes':dict(SentTimestamp='1657297206893'),
            'timestamp':'2022-07-08T16:20:06',
            'messageattributes':{'recorder':{'Type':'String','Value':'TRAINING'},
                'recordingId':{'Type':'String','Value':'TrainingRecorder-b8f0525f-706c-431a-a93b-0e9a8b66fd6c'},
                'deviceId':{'Type':'String','Value':'DATANAUTS_DEV_01'},
                'tenant':{'Type':'String','Value':'datanauts'}
            },
            'deviceid':None,
            'tenant':'datanauts',
            'topicarn':'dev-video-footage-events',
            'message':{'streamName': 'datanauts_DATANAUTS_DEV_01_TrainingRecorder', 
                'footageFrom': 1657297040802, 
                'footageTo': 1657297074110, 
                'uploadStarted': 1657297192968, 
                'uploadFinished': 1657297196322
            },
            'body':'{\n  "Type" : "Notification",\n  "MessageId" : "d172193a-1f8c-5487-9867-427844fa2dce",\n  "TopicArn" : "arn:aws:sns:eu-central-1:213279581081:dev-video-footage-events",\n  "Message" : "{\\"streamName\\":\\"datanauts_DATANAUTS_DEV_01_TrainingRecorder\\",\\"footageFrom\\":1657297040802,\\"footageTo\\":1657297074110,\\"uploadStarted\\":1657297192968,\\"uploadFinished\\":1657297196322}",\n  "Timestamp" : "2022-07-08T16:20:06.865Z",\n  "SignatureVersion" : "1",\n  "Signature" : "nAsyjqO90ZlZr/SF99sqeDy5wU8zbduZrPiSN+KeG88lzpEvIrytXie2TdC0LHqoKtp2FAKdO91bXyaSJthQLO/hmlnTX5GMG7H2wbnBDuELt2hQNdbU0Y1Jjg6d8DFcxVAVfV0KoUM6uBAiQdMYu+AxqshDL16/sB44TLcSujx3zXAdaS/6bPmayFu7ojWTQWZ0cUQQajzgRCb1OvT73utLSNQaAFh+2F3WPIk6gM1nyoKdii5X53CduO2GWjjX6sF9CqHANA+mJZ4dcD6KyG0SUrOqD/q4PRCVsyTNUd5oJ5+J9XfNnGZ4L1QgtTX36zkFsGjYQhVWiYVs6tVoRg==",\n  "SigningCertURL" : "https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",\n  "UnsubscribeURL" : "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:213279581081:dev-video-footage-events:e6853099-4723-45cc-b9ed-751484360ce8",\n  "MessageAttributes" : {\n    "recorder" : {"Type":"String","Value":"TRAINING"},\n    "recordingId" : {"Type":"String","Value":"TrainingRecorder-b8f0525f-706c-431a-a93b-0e9a8b66fd6c"},\n    "deviceId" : {"Type":"String","Value":"DATANAUTS_DEV_01"},\n    "tenant" : {"Type":"String","Value":"datanauts"}\n  }\n}'
        }
        obj = Message(sqs_message_queue_download)
        for field in self.fields:
            if field == "timestamp": # special case, datetime comes formatted
                aux_timestamp = expected["timestamp"]
                aux_timestamp = datetime.strptime(aux_timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
                timestamp = datetime.fromtimestamp(aux_timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S')
                assert obj.timestamp == timestamp
            
            elif field == "body": # special case, body comes wrapped as string
                aux_body = json.loads(expected["body"].replace("\'", "\""))
                assert obj.body == aux_body
            
            else:
                assert eval(f"obj.{field}") == expected[f"{field}"]

@pytest.mark.unit
class TestVideoMessage:
    fields = ["messageid", "receipthandle", "attributes", "timestamp", "messageattributes", "message",\
        "tenant", "topicarn", "deviceid", "body", "streamname", "recording_type", "recordingid", "footagefrom",\
        "footageto", "uploadstarted","uploadfinished"]

    def test_empty_input(self):
        sqs_message = dict()
        obj = VideoMessage(sqs_message)
        for field in self.fields:
            assert not eval(f"obj.{field}")

    def test_valid_input(self):
        expected = {
            'messageid':'6615420b-68df-4b65-94e3-8e4ed63935f4',
            'receipthandle':'AQEBhAkFOkQgJridT/VXqegxOFUc9D8VpMaclfNDlkOw1GJ78ocsIWRujqkYFSm1hfc11vC9i9Yjjspw6Qnq4qfpv1DWCX17ciEz1N5wZDmUa6o56RlHUwSBJLcnY3URXmEaXMM3rGnUkYV0x6G9GWOw4oV+BOnFH6TnUvDS/UBlDax/zGPqPOz6kqYkHP8MS7fn4FuwTr9LuBr4m/bxUcThgmyNaBk7piDgpChbd9PZ2G6zmt3axqYoaGummttEzabOrt1I+IWxO+Yst5MufZiY5/DZlIwj3Jng/ubXT4HY038S9JGZSXKjFMM3L2wkQ4MEjwANOXKJtv8/x/HbBhcqUoCuOdhee/7gKHpczMUNkHZ1rQAgY0l1UzxQt1OG6AMe7Os5SBhQ9g2O5LNgJ+cHgAVDJwbtV7xPFwN6Q3NeFIQ=',
            'attributes':dict(SentTimestamp='1657297206893'),
            'timestamp':'2022-07-08T16:20:06',
            'messageattributes':{'recorder':{'Type':'String','Value':'TRAINING'},
                'recordingId':{'Type':'String','Value':'TrainingRecorder-b8f0525f-706c-431a-a93b-0e9a8b66fd6c'},
                'deviceId':{'Type':'String','Value':'DATANAUTS_DEV_01'},
                'tenant':{'Type':'String','Value':'datanauts'}
            },
            'deviceid':"DATANAUTS_DEV_01",
            'tenant':'datanauts',
            'topicarn':'dev-video-footage-events',
            'message':{'streamName':'datanauts_DATANAUTS_DEV_01_TrainingRecorder',
                'footageFrom':1657297040802,
                'footageTo':1657297074110,
                'uploadStarted':1657297192968,
                'uploadFinished':1657297196322
            },
            'body':'{\n  "Type" : "Notification",\n  "MessageId" : "d172193a-1f8c-5487-9867-427844fa2dce",\n  "TopicArn" : "arn:aws:sns:eu-central-1:213279581081:dev-video-footage-events",\n  "Message" : "{\\"streamName\\":\\"datanauts_DATANAUTS_DEV_01_TrainingRecorder\\",\\"footageFrom\\":1657297040802,\\"footageTo\\":1657297074110,\\"uploadStarted\\":1657297192968,\\"uploadFinished\\":1657297196322}",\n  "Timestamp" : "2022-07-08T16:20:06.865Z",\n  "SignatureVersion" : "1",\n  "Signature" : "nAsyjqO90ZlZr/SF99sqeDy5wU8zbduZrPiSN+KeG88lzpEvIrytXie2TdC0LHqoKtp2FAKdO91bXyaSJthQLO/hmlnTX5GMG7H2wbnBDuELt2hQNdbU0Y1Jjg6d8DFcxVAVfV0KoUM6uBAiQdMYu+AxqshDL16/sB44TLcSujx3zXAdaS/6bPmayFu7ojWTQWZ0cUQQajzgRCb1OvT73utLSNQaAFh+2F3WPIk6gM1nyoKdii5X53CduO2GWjjX6sF9CqHANA+mJZ4dcD6KyG0SUrOqD/q4PRCVsyTNUd5oJ5+J9XfNnGZ4L1QgtTX36zkFsGjYQhVWiYVs6tVoRg==",\n  "SigningCertURL" : "https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",\n  "UnsubscribeURL" : "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:213279581081:dev-video-footage-events:e6853099-4723-45cc-b9ed-751484360ce8",\n  "MessageAttributes" : {\n    "recorder" : {"Type":"String","Value":"TRAINING"},\n    "recordingId" : {"Type":"String","Value":"TrainingRecorder-b8f0525f-706c-431a-a93b-0e9a8b66fd6c"},\n    "deviceId" : {"Type":"String","Value":"DATANAUTS_DEV_01"},\n    "tenant" : {"Type":"String","Value":"datanauts"}\n  }\n}',
            "streamname":'datanauts_DATANAUTS_DEV_01_TrainingRecorder',
            "recording_type":"TrainingRecorder",
            "recordingid":"TrainingRecorder-b8f0525f-706c-431a-a93b-0e9a8b66fd6c",
            "footagefrom":1657297040802,
            "footageto":1657297074110,
            "uploadstarted":datetime.fromtimestamp(1657297192968/1000.0),
            "uploadfinished":datetime.fromtimestamp(1657297196322/1000.0),
        }
        obj = VideoMessage(sqs_message_queue_download)
        for field in self.fields:
            if field == "timestamp": # special case, datetime comes formatted
                aux_timestamp = expected["timestamp"]
                aux_timestamp = datetime.strptime(aux_timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
                timestamp = datetime.fromtimestamp(aux_timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S')
                assert obj.timestamp == timestamp
            elif field == "body": # special case, body comes wrapped as string
                aux_body = json.loads(expected["body"].replace("\'", "\""))
                assert obj.body == aux_body
            else:
                assert eval(f"obj.{field}") == expected[f"{field}"]

@pytest.mark.unit
class TestSnapshotMessage:
    fields = ["messageid", "receipthandle", "attributes", "timestamp", "messageattributes", "message",\
        "tenant", "topicarn", "deviceid", "body", "chunks", "senttimestamp","eventtype"]

    def test_empty_input(self):
        sqs_message = dict()
        obj = SnapshotMessage(sqs_message)
        for field in self.fields:
            assert not eval(f"obj.{field}")

    def test_valid_input(self):
        expected = {
            'messageid':"8e8d79bb-8a6b-4461-a94d-51c7aa9bb0f1",
            'receipthandle':"AQEBtCBwuG54U0wqc572u5hzyBPmxsd7I+cOT7zAsKG4foVAvgYTG5UDz3DZ07IG6t61OKtInqBeOGpLA0AhQOPdHum2hRXA8RHwu7iznWK6wll/mZGKtLIyPHUG5NflRllxCY698TeVxyqvGgKqWqTQipV1XOTGpxw4X+5L1ScIsTdlLrj1KAGl61h5s+ZEOOvoQiMdP2ecYshf9LSul80BLdNe8l5iAsg/JJwODYhv/hubXSAKjDrLuDxjPQNq8PaCP+WC5h0iWeP3WsPfq1uXvulkPxdm8PGADJJuBw9vwSoHqociFlDP08hYZDqzi5EMDe8+PCoC26l4V9oQ/cWq8k7Peh2nsBulp2f6NIWfen9/5YDkkNVrwMy8MXmmUg7YNPVboGjgeVAfrExighxI+o4/KqgqonU1Qznd44r0nYU=",
            'attributes':{ "SentTimestamp": "1657624627631" },
            'timestamp':"2022-07-12T11:17:07",
            'messageattributes':{
                "deviceType": { "Type": "String", "Value": "hailysharey" },
                "operationMode": { "Type": "String", "Value": "SAFETY_CALL" },
                "eventType": {
                    "Type": "String",
                    "Value": "com.bosch.ivs.videorecorder.UploadRecordingEvent"
                },
                "tenant": { "Type": "String", "Value": "TEST_TENANT" },
                "subjectId": {
                    "Type": "String",
                    "Value": "15afc9be-5e9f-4ca0-b5bd-795420876b09"
                }
                },
            'deviceid':"srx_herbie_test_sim_am_01",
            'tenant':"TEST_TENANT",
            'topicarn':"prod-inputEventsTerraform",
            'message':{
                "topic": "com.bosch.rcc/srx_herbie_test_sim_am_01/things/twin/events/modified",
                "headers": {
                    "orig_adapter": "hono-mqtt",
                    "qos": "1",
                    "device_id": "com.bosch.rcc:srx_herbie_test_sim_am_01",
                    "creation-time": "1657624627540",
                    "traceparent": "00-0a84e899503b765b0ca9c96d86390022-98345f5e303cee11-00",
                    "kafka.timestamp": "1657624627540",
                    "kafka.topic": "hono.telemetry.tbccf1729c0ac4f748ef8c4a62a953076_hub",
                    "orig_address": "telemetry",
                    "kafka.key": "com.bosch.rcc:srx_herbie_test_sim_am_01",
                    "ditto-originator": "integration:bccf1729-c0ac-4f74-8ef8-c4a62a953076_things:hub",
                    "response-required": False,
                    "version": 2,
                    "requested-acks": [],
                    "content-type": "application/json",
                    "correlation-id": "2c17e5c9-e687-42a4-914e-1fdac9d42af3"
                },
                "path": "/features/com.bosch.ivs.videorecorder.UploadRecordingEvent",
                "value": {
                    "properties": {
                    "header": {
                        "message_type": "com.bosch.ivs.videorecorder.UploadRecordingEvent",
                        "timestamp_ms": 1657624627537,
                        "message_id": "de13f5cc-dfea-46da-8d89-9e3884e30a24",
                        "device_id": "srx_herbie_test_sim_am_01",
                        "boot_id": "d20ba602-2c06-4405-8ed5-db941573dbbe"
                    },
                    "correlation_id": "bda19c79-eac4-4d87-af31-632208257ab3",
                    "recording_id": "TrainingMultiSnapshot-bc4d26db-9689-4d77-b173-a21fb26776fa",
                    "recorder_name": "TrainingMultiSnapshot",
                    "command_status": {
                        "status_code": "COMMAND_STATUS_CODE__OK",
                        "details": "COMMAND_STATUS_CODE__OK"
                    }
                    }
                },
                "extra": {
                    "attributes": {
                    "vin": "unknown",
                    "tenant": "TEST_TENANT",
                    "vehicleType": "CARMODEL__FIAT_500_312",
                    "subjectId": "15afc9be-5e9f-4ca0-b5bd-795420876b09",
                    "operationMode": "SAFETY_CALL",
                    "deviceType": "hailysharey"
                    }
                },
                "revision": 4206,
                "timestamp": "2022-07-12T11:17:07.551341075Z"
                },
            'body':'{\n  "Type" : "Notification",\n  "MessageId" : "a24584ea-fe55-56a9-bd2f-dfada4266b2c",\n  "TopicArn" : "arn:aws:sns:eu-central-1:736745337734:prod-inputEventsTerraform",\n  "Message" : "{\\"topic\\":\\"com.bosch.rcc/srx_herbie_test_sim_am_01/things/twin/events/modified\\",\\"headers\\":{\\"orig_adapter\\":\\"hono-mqtt\\",\\"qos\\":\\"1\\",\\"device_id\\":\\"com.bosch.rcc:srx_herbie_test_sim_am_01\\",\\"creation-time\\":\\"1657624627540\\",\\"traceparent\\":\\"00-0a84e899503b765b0ca9c96d86390022-98345f5e303cee11-00\\",\\"kafka.timestamp\\":\\"1657624627540\\",\\"kafka.topic\\":\\"hono.telemetry.tbccf1729c0ac4f748ef8c4a62a953076_hub\\",\\"orig_address\\":\\"telemetry\\",\\"kafka.key\\":\\"com.bosch.rcc:srx_herbie_test_sim_am_01\\",\\"ditto-originator\\":\\"integration:bccf1729-c0ac-4f74-8ef8-c4a62a953076_things:hub\\",\\"response-required\\":false,\\"version\\":2,\\"requested-acks\\":[],\\"content-type\\":\\"application/json\\",\\"correlation-id\\":\\"2c17e5c9-e687-42a4-914e-1fdac9d42af3\\"},\\"path\\":\\"/features/com.bosch.ivs.videorecorder.UploadRecordingEvent\\",\\"value\\":{\\"properties\\":{\\"header\\":{\\"message_type\\":\\"com.bosch.ivs.videorecorder.UploadRecordingEvent\\",\\"timestamp_ms\\":1657624627537,\\"message_id\\":\\"de13f5cc-dfea-46da-8d89-9e3884e30a24\\",\\"device_id\\":\\"srx_herbie_test_sim_am_01\\",\\"boot_id\\":\\"d20ba602-2c06-4405-8ed5-db941573dbbe\\"},\\"correlation_id\\":\\"bda19c79-eac4-4d87-af31-632208257ab3\\",\\"recording_id\\":\\"TrainingMultiSnapshot-bc4d26db-9689-4d77-b173-a21fb26776fa\\",\\"recorder_name\\":\\"TrainingMultiSnapshot\\",\\"command_status\\":{\\"status_code\\":\\"COMMAND_STATUS_CODE__OK\\",\\"details\\":\\"COMMAND_STATUS_CODE__OK\\"}}},\\"extra\\":{\\"attributes\\":{\\"vin\\":\\"unknown\\",\\"tenant\\":\\"TEST_TENANT\\",\\"vehicleType\\":\\"CARMODEL__FIAT_500_312\\",\\"subjectId\\":\\"15afc9be-5e9f-4ca0-b5bd-795420876b09\\",\\"operationMode\\":\\"SAFETY_CALL\\",\\"deviceType\\":\\"hailysharey\\"}},\\"revision\\":4206,\\"timestamp\\":\\"2022-07-12T11:17:07.551341075Z\\"}",\n  "Timestamp" : "2022-07-12T11:17:07.585Z",\n  "SignatureVersion" : "1",\n  "Signature" : "p8ZAMYlyEkbAG+Bg31+ZmUVvw4LN7FC40JoGmGD1Cye7qJh4dXLIuptICLYvMgcYjeb5SVEavW7DWm7dKALGYuRnJH5ffy4Kwd7P6P63IOD1tLHvYMpGSxwSJX4gvgBW6juqkd3lqLrwOVQ21PgGVtdFI/xD8BK7UhDutQZKtWhDXVAWpV1Bqls0m6edOoGu+yhfBwOGG7Kv+AYXQ63xgKseZnsLCRMU5KU4la10Ik/zjk6HSgsI2iWE5hEK1nMWH+Lrb4gxfLjnOGJaUyTxUc6c5uapWPnytc9bKz9/ARqaXgV146nzzr86vjlk26bRayLX9Q5H6mm/wTmQPg1uDw==",\n  "SigningCertURL" : "https://sns.eu-central-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",\n  "UnsubscribeURL" : "https://sns.eu-central-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:eu-central-1:736745337734:prod-inputEventsTerraform:23a2a7a5-1fd3-4bbe-b766-1da13b0577d9",\n  "MessageAttributes" : {\n    "deviceType" : {"Type":"String","Value":"hailysharey"},\n    "operationMode" : {"Type":"String","Value":"SAFETY_CALL"},\n    "eventType" : {"Type":"String","Value":"com.bosch.ivs.videorecorder.UploadRecordingEvent"},\n    "tenant" : {"Type":"String","Value":"TEST_TENANT"},\n    "subjectId" : {"Type":"String","Value":"15afc9be-5e9f-4ca0-b5bd-795420876b09"}\n  }\n}',
            'chunks':[],
            'senttimestamp':datetime.fromtimestamp(int(1657624627631)/1000.0),
            'eventtype':'UploadRecordingEvent'
            }

        obj = SnapshotMessage(sqs_message_queue_selector)
        for field in self.fields:
            if field == "timestamp": # special case, datetime comes formatted
                aux_timestamp = expected["timestamp"]
                aux_timestamp = datetime.strptime(aux_timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
                timestamp = datetime.fromtimestamp(aux_timestamp/1000.0).strftime('%Y-%m-%d %H:%M:%S')
                assert obj.timestamp == timestamp
            elif field == "body": # special case, body comes wrapped as string
                aux_body = json.loads(expected["body"].replace("\'", "\""))
                assert obj.body == aux_body
            elif field == "senttimestamp":
                aux_senttimestamp = expected["senttimestamp"]
                assert obj.senttimestamp == aux_senttimestamp
            else:
                assert eval(f"obj.{field}") == expected[f"{field}"]
