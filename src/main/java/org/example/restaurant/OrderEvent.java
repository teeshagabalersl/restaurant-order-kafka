package org.example.restaurant;

public class OrderEvent {

    private String orderId;
    private String restaurantId;
    private String customerName;
    private String items;
    private String orderTime;
    private String orderStatus;

    public OrderEvent(String orderId, String restaurantId,
                      String customerName, String items,
                      String orderTime, String orderStatus) {
        this.orderId = orderId;
        this.restaurantId = restaurantId;
        this.customerName = customerName;
        this.items = items;
        this.orderTime = orderTime;
        this.orderStatus = orderStatus;
    }

    public String toJson() {
        return String.format(
                "{\n" +
                        "  \"order_id\": \"%s\",\n" +
                        "  \"restaurant_id\": \"%s\",\n" +
                        "  \"customer_name\": \"%s\",\n" +
                        "  \"items\": \"%s\",\n" +
                        "  \"order_time\": \"%s\",\n" +
                        "  \"order_status\": \"%s\"\n" +
                        "}",
                orderId, restaurantId, customerName,
                items, orderTime, orderStatus
        );
    }
}
