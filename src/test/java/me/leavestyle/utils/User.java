package me.leavestyle.utils;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class User {

    private String userId;

    private String userName;

    private String userAddress;
}
