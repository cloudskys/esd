package com.cloudskys.untils;


public enum  EnumRoleOperation implements RoleOperation{

  ROLE_ROOT_ADMIN{
        @Override
        public String op(String sid){
          return "ROLE_ROOT_ADMIN"+sid;
        }
    },
    ROLE_ORDER_ADMIN{
        @Override
        public String op(String sid){
            return "ROLE_ORDER_ADMIN"+sid;
        }
    },
    ROLE_NORMAL_ADMIN{
        @Override
        public String op(String sid){
            return "ROLE_NORMAL_ADMIN"+sid;
        }
    };
}