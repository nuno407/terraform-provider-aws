"""A wrapper arround io.BytesIO"""
import io


class UnclosableMemoryBuffer(io.BytesIO):
    """
    UnclosableMemoryBuffer

    Overwrites the close method of BytesIO to do nothing, this is needed if the writer attempts to close it.
    This is the case of the pandas.to_parquet method, which closes the buffer after writing to it.

    fastparquet issue: https://github.com/dask/fastparquet/issues/868
    """

    def close(self):
        """
        Overwrites the close method to do nothing, becoming an unclosable buffer
        """

    def __del__(self):
        super().close()
        super().__del__()
