package wdsr.exercise5.dao;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import wdsr.exercise5.model.Trade;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Types;
import java.sql.Date;
import java.util.Optional;

@Repository
public class TradeDao {

    @Autowired
    JdbcTemplate jdbcTemplate;

    private final RowMapper<Trade> mapper = (resultSet, rowNum) -> new Trade(resultSet.getInt("id"),
    		 resultSet.getString("asset"), resultSet.getDouble("amount"), resultSet.getDate("date"));
    
    /**
     * Zaimplementuj metode insertTrade aby wstawiała nowy rekord do tabeli "trade"
     * na podstawie przekazanego objektu klasy Trade.
     * @param trade
     * @return metoda powinna zwracać id nowego rekordu.
     */
    public int insertTrade(Trade trade) {
    	String query = "INSERT INTO trade(asset, amount, date) values(?, ?, ?)";
    	 
    	 KeyHolder keyHolder = new GeneratedKeyHolder();
    	 jdbcTemplate.update(con -> {
    		 PreparedStatement preparedStatement = con.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS);
    		 preparedStatement.setString(1, trade.getAsset());
    		 preparedStatement.setDouble(2, trade.getAmount());
    		 preparedStatement.setDate(3, new Date(trade.getDate().getTime()));
    		 return preparedStatement;
    	 }, keyHolder);
    	 
    	 return keyHolder.getKey().intValue();
    }

    /**
     * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id.
     * Użyj intrfejsu RowMapper.
     * @param id
     * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym id.
     */
    public Optional<Trade> extractTrade(int id) {
    	String query = "select * from trade where id = ?";
    	Trade trade = jdbcTemplate.queryForObject(query, new Object[] { id }, mapper);
    	return Optional.ofNullable(trade);
    }

    /**
     * Zaimplementuj metode aby wyciągneła z bazy rekord o podanym id.
     * @param id, rch - callback który przetworzy wyciągnięty wiersz.
     * @return metaoda powinna zwracać obiekt reprezentujący rekord o podanym id.
     */
    public void extractTrade(int id, RowCallbackHandler rch) {
    	String query = "select * from trade where id=" + id;
    	jdbcTemplate.query(query, rch);
    }

    /**
     * Zaimplementuj metode aby zaktualizowała rekord o podanym id danymi z przekazanego parametru 'trade'
     * @param trade
     */
    public void updateTrade(int id, Trade trade) {
    	String query = "update trade "
    			+ "set asset = ?, "
    			+ "amount = ?, "
    			+ "date = ? "
    			+ "WHERE ID = ?";
    	jdbcTemplate.update(query, new Object[] { trade.getAsset(), trade.getAmount(), trade.getDate(), id },
    	 		new int[] { Types.VARCHAR, Types.DOUBLE, Types.DATE, Types.INTEGER });
    }

    /**
     * Zaimplementuj metode aby usuwała z bazy rekord o podanym id.
     * @param id
     */
    public void deleteTrade(int id) {
    	String query = "delete from trade "
    			+ "where id = ?";
    	jdbcTemplate.update(query, id);
    }

}
