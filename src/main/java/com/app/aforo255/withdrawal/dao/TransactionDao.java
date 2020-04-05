package com.app.aforo255.withdrawal.dao;

import org.springframework.data.repository.CrudRepository;

import com.app.aforo255.withdrawal.domain.Transaction;

public interface TransactionDao extends CrudRepository<Transaction, Integer>{

	
}
