#include <avdweb_VirtualDelay.h>
#include "Servo.h"
#include <Wire.h>
#include "SparkFun_APDS9960.h"


// Timer Variables and Constants
bool bool01;
VirtualDelay Timer01; // timer pomiarowy
VirtualDelay TimerA;
VirtualDelay TimerR;
VirtualDelay TimerG;
VirtualDelay TimerB;


// Servo Variables and Constants
Servo Chwytak;
int SERVO_pos = 0;
#define WAIT_PIN 8
#define IN_12V_SERVO_CLOSE 2
#define IN_12V_SERVO_OPEN 5
#define SERVO_PIN_CHWYTAK 3


// TRyBUS Communication Variables and Constants
Servo TRyBUS_01;
#define TRyBUS_PIN 4
uint16_t a, r, g, b;
#define SEGMENTY 40.0
#define ZAKRES_DOLNY 0.0
#define ZAKRES_GORNY 200.0
#define ZAKRES_DOLNY2 0.0
#define ZAKRES_GORNY2 4000.0
#define OFSTA 1000
#define OFSTR 5500
#define OFSTG 10000
#define OFSTB 14500
#define OFST 500
#define _M_ 1.0/((ZAKRES_GORNY-ZAKRES_DOLNY)/(ZAKRES_GORNY2-ZAKRES_DOLNY2))


// Sensor Variables and Constants
SparkFun_APDS9960 apds = SparkFun_APDS9960();
uint16_t ambient_light = 0;
uint16_t red_light = 0;
uint16_t green_light = 0;
uint16_t blue_light = 0;


void setup() {
  Serial.begin(4800);  // sensor setup
  pinMode(LED_BUILTIN, OUTPUT);
  pinMode(WAIT_PIN, OUTPUT);
  
  digitalWrite(LED_BUILTIN, LOW);
  digitalWrite(WAIT_PIN, LOW);

  // Initialize APDS-9960 (configure I2C and initial values)
  if ( apds.init() ) {
    Serial.println(F("APDS-9960 initialization complete"));
  } else {
    Serial.println(F("Something went wrong during APDS-9960 init!"));
  }

  // Start running the APDS-9960 light sensor (no interrupts)
  if ( apds.enableLightSensor(false) ) {
    Serial.println(F("Light sensor is now running"));
  } else {
    Serial.println(F("Something went wrong during light sensor init!"));
  }
  

  // servo setup
  Chwytak.attach(SERVO_PIN_CHWYTAK);
  Chwytak.writeMicroseconds(700);
  pinMode(IN_12V_SERVO_CLOSE, INPUT_PULLUP);
  pinMode(IN_12V_SERVO_OPEN, INPUT_PULLUP);

  pinMode(TRyBUS_PIN, OUTPUT);
}

void loop() {
  Timer01.start(150);
  TimerB.start(200);


  if(!digitalRead(IN_12V_SERVO_OPEN)){
    SERVO_pos = 1;
    TimerB.running = 0;
  }
  if(!digitalRead(IN_12V_SERVO_CLOSE)){
    SERVO_pos = 0;
    TimerB.running = 0;
  }
  if(digitalRead(IN_12V_SERVO_OPEN)&&digitalRead(IN_12V_SERVO_CLOSE)&&TimerB.elapsed()){
    //digitalWrite(TRyBUS_PIN, LOW); digitalWrite(LED_BUILTIN, LOW);
    SERVO_pos = 2;
  }

  switch(SERVO_pos){
    case 0: //pomiar
      Chwytak.writeMicroseconds(700);
      digitalWrite(WAIT_PIN, LOW);
      
      if(Timer01.elapsed()){     
        apds.readAmbientLight(ambient_light);

        if(ambient_light < 35){ digitalWrite(TRyBUS_PIN, LOW); digitalWrite(LED_BUILTIN, LOW);}
        if(ambient_light > 150){ digitalWrite(TRyBUS_PIN, HIGH); digitalWrite(LED_BUILTIN, HIGH);}
        }

    break;
    case 1: //zamykanie
      digitalWrite(WAIT_PIN, LOW);
      digitalWrite(TRyBUS_PIN, LOW); digitalWrite(LED_BUILTIN, LOW); // Wyłączenie transoptora krańcówki (przerwanie sygnału, obejście zabezpieczenia osi T)
      Chwytak.writeMicroseconds(2400);
    break;
    case 2: //czekanie
      TimerA.start(500);
      if(TimerA.elapsed()){
        digitalWrite(WAIT_PIN, !digitalRead(WAIT_PIN));
      }
      digitalWrite(TRyBUS_PIN, LOW); digitalWrite(LED_BUILTIN, LOW);
    break;
  }

}
