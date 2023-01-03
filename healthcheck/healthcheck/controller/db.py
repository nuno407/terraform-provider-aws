"""database controller module."""
from kink import inject
from jsonschema import ValidationError

from healthcheck.model import Artifact, ArtifactType
from healthcheck.database import DBClient, DBCollection
from healthcheck.exceptions import (FailDocumentValidation, NotPresentError,
                                    NotYetIngestedError)
from healthcheck.schema.validator import DocumentValidator, Schema


@inject
class DatabaseController():
    """Healthcheck database controller."""

    def __init__(
        self,
        db_client: DBClient,
        schema_validator: DocumentValidator
    ) -> None:
        self.__db_client = db_client
        self.__schema_validator = schema_validator

    def __db_find_one_or_raise(self, artifact: Artifact, collection: DBCollection, query: dict) -> dict:
        """
        Executes a find query and returns the result, if only one entry was found.
        Args:
            collection (DBCollection): The specified collection
            query (dict): The query to be executed

        Raises:
            FailDocumentValidation: If no entries were found or more then one entries were found.

        Returns:
            dict: The result of the query.
        """
        docs = self.__db_client.find_many(collection, query)
        num_docs = len(docs)

        if num_docs == 0:
            raise FailDocumentValidation(artifact, f"Collection {collection} document not found")
        if num_docs > 1:
            raise FailDocumentValidation(
                artifact, f"More then one recordings document exist in collection {collection}")
        return docs[0]

    def is_signals_doc_valid_or_raise(self, artifact: Artifact) -> list:
        """
        Validate all signal documents for the specified recording.
        An exception is raised ifthe validaiton wasn't sucessfull.

        Args:
            artifact (Artifact): The artifact to verify

        Raises:
            FailDocumentValidation: If the validation fails or the query result was empty.

        Returns:
            list: The result of the query.
        """
        recording_id = artifact.artifact_id
        docs = self.__db_client.find_many(DBCollection.SIGNALS, {"recording": recording_id})
        num_docs = len(docs)

        if num_docs == 0:
            raise FailDocumentValidation(artifact, "Signals document not found")
        for doc in docs:
            try:
                self.__schema_validator.validate_document(doc, Schema.SIGNALS)
            except ValidationError as err:
                raise FailDocumentValidation(artifact, message=err.message, json_path=err.json_path) from err

        return docs

    def is_recordings_doc_valid_or_raise(self, artifact: Artifact) -> dict:
        """
        Validate the recording document for the specified video_id.
        An exception is raised if the validaiton wasn't sucessfull.

        Args:
            artifact (Artifact): The artifact to verify

        Raises:
            FailDocumentValidation: If the validation fails or the query result length is diferent then one.

        Returns:
            dict: The result of the query.
        """
        video_id = artifact.artifact_id
        doc = self.__db_find_one_or_raise(artifact, DBCollection.RECORDINGS, {"video_id": video_id})

        schema = Schema.RECORDINGS_SNAPSHOT if artifact.artifact_type == ArtifactType.SNAPSHOT else Schema.RECORDINGS
        try:
            self.__schema_validator.validate_document(doc, schema)
        except ValidationError as err:
            raise FailDocumentValidation(artifact, message=err.message, json_path=err.json_path) from err

        return doc

    def is_pipeline_execution_and_algorithm_output_doc_valid_or_raise(self, artifact: Artifact) -> list:
        """
        Validate the pipeline_execution and algorithm_output collection for the specified video_id.
        It will look for the processing stepsthat should be made in pipeline_execution and search for
        the output of those in algorithm_output collection.

        An exception is raised if the validaiton wasn't sucessfull.

        Args:
            artifact (Artifact): The artifact to be used on the query.

        Raises:
            FailDocumentValidation: If the validation fails or the number of documents does not macth the expected.

        Returns:
            dict: The result of the algorithm_output collection.
        """
        video_id = artifact.artifact_id

        # Validate Pipeline execution
        doc_execution = self.__db_find_one_or_raise(artifact, DBCollection.PIPELINE_EXECUTION, {"_id": video_id})
        try:
            self.__schema_validator.validate_document(doc_execution, Schema.PIPELINE_EXECUTION)
        except ValidationError as err:
            raise FailDocumentValidation(artifact, message=err.message, json_path=err.json_path) from err

        # Validate algorithm output for all pipeline execution
        for process in doc_execution["processing_list"]:
            doc_id = f"{video_id}_{process}"
            doc_output = self.__db_find_one_or_raise(artifact, DBCollection.ALGORITHM_OUTPUT, {"_id": doc_id})
            try:
                self.__schema_validator.validate_document(doc_output, Schema.ALGORITHM_OUTPUT)
            except ValidationError as err:
                raise FailDocumentValidation(artifact, message=err.message, json_path=err.json_path) from err

        # Make sure it doesn't have different number of documents associated with the same ID
        docs_output = self.__db_client.find_many(DBCollection.ALGORITHM_OUTPUT, {"pipeline_id": video_id})
        num_docs_output = len(docs_output)
        num_process_steps = len(doc_execution["processing_list"])

        if num_docs_output != num_process_steps:
            raise FailDocumentValidation(
                artifact,
                f"The number of documents ({num_docs_output}) in {DBCollection.ALGORITHM_OUTPUT} " +
                f"is not the same as the number of processings steps ({num_process_steps})" +
                f"in {DBCollection.PIPELINE_EXECUTION}")

        return docs_output

    def is_data_status_complete_or_raise(self, artifact: Artifact) -> None:
        """checks if db entry exists in recordings collection for computed hash 'internal_message_reference_id'
        and if associated 'data_status' in the pipeline-execution is set to complete

        Args:
            artifact (Artifact): artifact that is being verified

        Raises:
            NotYetIngestedError: means the artifact was not ingested yet and we should wait a while
            NotPresentError: means there's an error with this artifact since recording was found but
                pipeline-execution was not found
        """
        internal_hash = artifact.internal_message_reference_id

        docs = self.__db_client.find_many(DBCollection.RECORDINGS, {
            "recording_overview.internal_message_reference_id": internal_hash
        })
        if len(docs) == 0:
            raise NotYetIngestedError(artifact, "Unable to find 'internal_message_reference_id'")

        recording_doc = docs[0]
        video_id = recording_doc["video_id"]

        docs = self.__db_client.find_many(DBCollection.PIPELINE_EXECUTION, {"_id": video_id})
        if len(docs) == 0:
            raise NotPresentError(artifact, f"Unable to find pipeline-execution entry with video_id {video_id}")

        pipeline_exec_doc = docs[0]
        data_status = pipeline_exec_doc["data_status"]

        if data_status != "complete":
            raise NotYetIngestedError(artifact, "Ingestion of the artifact is not yet complete")
