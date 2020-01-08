package com.tecgurus.demo.kafka.consumer;

import com.tecgurus.demo.kafka.EmpleadoDTO;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ConsumidorMensajes {


    @KafkaListener(topics = "Topic1", groupId = "Group1")
    public void listen(String message) {
        //TODO ya se recibio el mensaje, parseo y guarda
        JSONObject mensajeRecibido = new JSONObject(message);
        EmpleadoDTO empleado = new EmpleadoDTO();
        empleado.setCorreo(mensajeRecibido.getString("correo"));
        empleado.setImagenB64(mensajeRecibido.getString("imagen"));
        empleado.setNombre(mensajeRecibido.getString("nombre"));
        log.info("Empleado recibido: {}", empleado);
    }

}
