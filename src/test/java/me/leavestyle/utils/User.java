package me.leavestyle.utils;

import lombok.*;

@Getter
@Builder
@ToString
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {

    private String userId;

    private String userName;

    private String userAddress;
}
