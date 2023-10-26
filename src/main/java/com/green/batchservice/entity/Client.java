package com.green.batchservice.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;

@Entity(name = "client")
@Data
@AllArgsConstructor
@NoArgsConstructor
@DynamicUpdate
public class Client {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "cli_id")
    private long id;

    @Column(name = "cli_first_name")
    private String firstName;

    @Column(name = "cli_last_name")
    private String lastName;

    @Column(name = "cli_city")
    private String city;

    @Column(name = "cli_zip_code")
    private long zipCode;

    @Column(name = "cli_phone_num")
    private long phoneNum;

}
