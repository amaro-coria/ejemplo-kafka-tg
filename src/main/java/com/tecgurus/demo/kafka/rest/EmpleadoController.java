package com.tecgurus.demo.kafka.rest;

import com.tecgurus.demo.kafka.EmpleadoDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping(value = "/api/v1/kafka")
@Slf4j
public class EmpleadoController {

    @Autowired
    private KafkaTemplate template;

    @PostMapping
    public ResponseEntity<String> addEmpleado(@RequestBody EmpleadoDTO dto) {
        //TOO enviar a kafka
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("nombre", dto.getNombre());
        jsonObject.put("imagen", dto.getImagenB64());
        jsonObject.put("correo", dto.getCorreo());
        ProducerRecord<String, String> record = new ProducerRecord<>("Topic1", jsonObject.toString());

        //ListenableFuture<SendResult> result = template.send(record);

        try {
            template.send(record).get(10, TimeUnit.SECONDS);
            handleSuccess(jsonObject.toString());
        } catch (ExecutionException e) {
            handleFailure(jsonObject.toString(), e.getCause());
        } catch (TimeoutException | InterruptedException e) {
            handleFailure(jsonObject.toString(), e);
        }
        return ResponseEntity.ok("Success");
    }


    @PostMapping(value = "/async")
    public ResponseEntity<String> addEmpleadoAsync(@RequestBody EmpleadoDTO dto) {
        //TOO enviar a kafka
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("nombre", dto.getNombre());
        jsonObject.put("imagen", dto.getImagenB64());
        jsonObject.put("correo", dto.getCorreo());
        ProducerRecord<String, String> record = new ProducerRecord<>("Topic1", jsonObject.toString());

        ListenableFuture<SendResult<String, Object>> future = template.send(record);

        future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {



            @Override
            public void onSuccess(SendResult<String, Object> result) {
                log.info("sent message='{}' with offset={}", jsonObject.toString(),
                        result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                log.error("unable to send message='{}'", jsonObject.toString(), ex);
            }
        });

        return ResponseEntity.ok("Success");
    }

    private void handleSuccess(String result) {
        System.out.println("OKOKOK");
        log.info("Success");
    }

    private void handleFailure(String data, Throwable ex) {
        System.out.println("NNNNNN");
        log.info("Failure");
    }

}
