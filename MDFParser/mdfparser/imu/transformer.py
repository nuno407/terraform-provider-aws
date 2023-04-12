"""IMU Transformer Class"""

import pandas as pd


class IMUTransformer:
    """
    Class the holds multiple transformations functions.
    """

    def apply_transformation(self, imu_data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the transformation to the DataFrame

        Args:
            imu_data (pd.DataFrame): Imu data to be transformed.

        Returns:
            pd.DataFrame: The transformed Dataframe.
        """
        return self.apply_window_transformation_stats(imu_data, "10ms", ["min", "max", "mean", "var"])

    @staticmethod
    def apply_avg_transformation(imu_data: pd.DataFrame, time_window: str) -> pd.DataFrame:
        """
        Apply resampling at a time_window precision (averaging the values in the milisecond window).

        Args:
            imu_data (pd.DataFrame): Imu data to be processed (The resample will be done on the timestamp field)
            time_window (str): The window to calculate the avg (Eg: "1ms")
                                (https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html)
        Returns:
            pd.DataFrame: The transformed Dataframe.
        """
        return imu_data.resample(time_window, on=time_window).mean().dropna().reset_index()

    @staticmethod
    def apply_window_transformation_stats(imu_data: pd.DataFrame, time_window: str, stats: list[str]) -> pd.DataFrame:
        """
        Apply resampling at time_window precision calculating multiple statistics specified in the stats field.

        Args:
            imu_data (pd.DataFrame): Imu data to be processed (The resample will be done on the timestamp field)
            time_window (str): The window to calculate the avg (Eg: "1ms")
                                (https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html)
            metrics (list[str]): A list of function names, (Eg: ["min","max"])
                                (https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.agg.html)
        Returns:
            pd.DataFrame: The transformed Dataframe.
        """
        result_df = imu_data.resample(
            time_window, on="timestamp").agg(
            func=stats).dropna().reset_index()  # type: ignore

        # Flat the multi index columns
        result_df.columns = ["_".join(col) if col[1] != "" else col[0]
                             for col in result_df.columns.to_flat_index()]  # type: ignore
        return result_df
