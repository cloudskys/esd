package com.cloudskys.untils;
/**
 * 
 * @author wangyt
 *
 */
public enum ContantEnum {
	SUCCESS("000"), FAIL("999"),TIMEOUT("400");
	private String code;

	private ContantEnum(String code) {
		this.code = code;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}
	public enum CouponStatusEnum{
		unuse("未使用"),lock("已锁定"),outdate("已过期"),usedone("已使用");
		private String code;
		private CouponStatusEnum(String code) {
			this.code = code;
		}

		public String getCode() {
			return code;
		}

		public void setCode(String code) {
			this.code = code;
		}
		
	}
	

}
