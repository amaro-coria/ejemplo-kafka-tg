package com.tecgurus.demo.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmpleadoDTO implements Serializable {

    private String nombre;
    private String imagenB64;
    private String correo;


}
