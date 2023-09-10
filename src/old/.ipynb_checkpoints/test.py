# adding new feature as suggested by Javier
pressure_train["Temperatura enfriamiento °C"] = (
    pressure_train.Temperature_Out_Degrees - pressure_train.Temperature_Ambient_Degrees
)
pressure_train["Temperatura enfriamiento °C"] = pressure_train[
    "Temperatura enfriamiento °C"
].apply(lambda x: max(0, x))
correlation_matrix = pressure_train[
    [
        "total_average_power_consumption_kW_nodes",
        "total_average_power_consumption_kW_chillers",
        "Temperature_In_Degrees",
        "Temperature_Out_Degrees",
        "Temperature_Ambient_Degrees",
        "Temperature_Evaporator_Degrees",
        "Temperatura enfriamiento °C",
        "max_pressure_Bars",
        "n_working_compressors",
    ]
].copy()
correlation_matrix.columns = [
    "Consumo total de los 288 nodos/ kW",
    "Consumo total de las 2 enfriadoras/ kW",
    "Temperatura de entrada a las enfriadoras/ °C",
    "Temperatura de salida a las enfriadoras/ °C",
    "Temperatura Ambiente/ °C",
    "Temperatura en el evaporador/ °C",
    "Temperatura enfriamiento °C",
    "Presion máxima en los 4 compresores/ bar",
    "Número de compresores funcionando",
]

# Plot the correlation heatmap
plt.figure(figsize=(10, 6))
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", center=0)
plt.title(
    "Matriz de correlación entre las distintas series temporales durante los veranos de 2018 a 2020"
)
plt.show()
