consumption_train_pressure = consumption_train[
    ["time", "max_pressure_Bars", "n_working_compressors"]
].copy()
consumption_train_pressure.set_index("time", inplace=True)
consumption_train_pressure.resample("H").max()
consumption_train_pressure.reset_index(inplace=True)
consumption_train = consumption_train[
    [
        "time",
        "total_average_power_consumption_kW_nodes",
        "total_average_power_consumption_kW_chillers",
        "Temperature_In_Degrees",
        "Temperature_Out_Degrees",
        "Temperature_Ambient_Degrees",
        "Temperature_Evaporator_Degrees",
    ]
].copy()
consumption_train.set_index("time", inplace=True)
consumption_train.resample("H").mean()
consumption_train.reset_index(inplace=True)
consumption_train = pd.merge(consumption_train, consumption_train_pressure, on="time")

pressure_train_pressure = pressure_train[
    ["time", "max_pressure_Bars", "n_working_compressors"]
].copy()
pressure_train_pressure.set_index("time", inplace=True)
pressure_train_pressure.resample("H").max()
pressure_train_pressure.reset_index(inplace=True)
pressure_train = pressure_train[
    [
        "time",
        "total_average_power_consumption_kW_nodes",
        "total_average_power_consumption_kW_chillers",
        "Temperature_In_Degrees",
        "Temperature_Out_Degrees",
        "Temperature_Ambient_Degrees",
        "Temperature_Evaporator_Degrees",
    ]
].copy()
pressure_train.set_index("time", inplace=True)
pressure_train.resample("H").mean()
pressure_train.reset_index(inplace=True)
pressure_train = pd.merge(pressure_train, pressure_train_pressure, on="time")
