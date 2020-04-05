package com.app.aforo255.withdrawal.domain;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@Entity
@Table(name = "transaction")
public class Transaction implements Serializable {

	private static final long serialVersionUID = 4913093L;

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)	
	private Integer id;
	@NotNull
	private double amount;
	@NotBlank
	@NotNull
	private String type ;
	@NotNull
	@Column(name="creation_date")
	private String creationDate ;
	@NotNull
	@Column(name="account_id")
	private int accountId ;
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	public double getAmount() {
		return amount;
	}
	public void setAmount(double amount) {
		this.amount = amount;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(String creationDate) {
		this.creationDate = creationDate;
	}
	public int getAccountId() {
		return accountId;
	}
	public void setAccountId(int accountId) {
		this.accountId = accountId;
	}
}
