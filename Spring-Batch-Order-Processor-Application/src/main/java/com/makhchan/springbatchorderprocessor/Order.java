package com.makhchan.springbatchorderprocessor;

public record Order(Long orderId, String customerName, Double amount) {
}