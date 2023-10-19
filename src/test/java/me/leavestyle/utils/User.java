package me.leavestyle.utils;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
@Data
public class User {

    private String userId;

    private String userName;

    private String userAddress;
}
