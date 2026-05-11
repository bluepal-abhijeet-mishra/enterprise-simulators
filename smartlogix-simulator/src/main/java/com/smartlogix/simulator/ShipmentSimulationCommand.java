package com.smartlogix.simulator;

import lombok.Data;

@Data
public class ShipmentSimulationCommand {

    private String shipmentId;

    private String originWarehouseId;

    private String destinationWarehouseId;
}
