package me.leavestyle.utils;

import lombok.*;

@Getter
@Builder
@ToString
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Model {

    private String id;

    private String name;

    private String status;
}
