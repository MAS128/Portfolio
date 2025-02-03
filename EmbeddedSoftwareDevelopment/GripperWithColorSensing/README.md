# Servo-Based Gripping Mechanism with Light Sensor Control

## Overview

This project implements an **embedded system** that controls a **servo-driven gripping mechanism**, using an **APDS-9960 light sensor** to determine actions based on ambient light intensity, color or gestures. The system uses **software timers**, **digital inputs**, and **TRyBUS communication** to operate efficiently.

## Features

- **Servo Control:** Three operating states (Measurement, Closing, and Waiting).
- **Light Sensing:** Detects ambient light levels and triggers actions.
- **State-Based Operation:** Responds to digital input signals for servo actuation.
- **Asynchronous Processing:** Uses **VirtualDelay** timers for efficient event handling.
- **Indicator Signals:** LED and TRyBUS communication output.

## Software Dependencies

- **[Servo.h](https://www.arduino.cc/en/Reference/Servo)** – For servo motor control, **!!the library used in this project has been modified!!**.
- **[avdweb_VirtualDelay.h](https://github.com/avandalen/avdweb_VirtualDelay)** – Non-blocking software timers.
- **[Wire.h](https://www.arduino.cc/en/Reference/Wire)** – I2C communication support.
- **[SparkFun_APDS9960.h](https://github.com/sparkfun/SparkFun_APDS-9960_RGB_and_Gesture_Sensor_Arduino_Library)** – Driver for the **APDS-9960 light sensor**.

## Operating States

| State  | Description |
|--------|------------|
| **0 (Measurement Mode)** | The system measures ambient light using APDS-9960. Based on the light level, it activates or deactivates the **TRyBUS signal**. |
| **1 (Closing Mode)** | Moves the **gripper (Chwytak) to a closed position (2400µs pulse width)**. |
| **2 (Waiting Mode)** | The system enters a standby mode, where the WAIT_PIN blinks periodically. |

## Logic Flow

1. The system starts **reading ambient light** every 150ms.
2. If the light is **too low (<35)**, the TRyBUS output is set **LOW**.
3. If the light is **too high (>150)**, the TRyBUS output is set **HIGH**.
4. When an **open/close input** is detected, the **servo position changes accordingly**.
5. The system then transitions between states **based on input signals and timer conditions**.

## Pin Configuration

| Pin | Function |
|-----|----------|
| **8** | WAIT_PIN (Output) |
| **2** | IN_12V_SERVO_CLOSE (Input) |
| **5** | IN_12V_SERVO_OPEN (Input) |
| **3** | SERVO_PIN_CHWYTAK (Servo Output) |
| **4** | TRyBUS_PIN (Communication Output) |

---
