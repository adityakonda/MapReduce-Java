package customsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Stock implements WritableComparable<Stock> {

	private String date;
	private String symbol;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(date);
		out.writeUTF(symbol);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub

		date = in.readUTF();
		symbol = in.readUTF();
	}

	@Override
	public int compareTo(Stock stock) {
		// TODO Auto-generated method stub

		int response = this.symbol.compareTo(stock.symbol);

		if (response == 0) {
			response = this.date.compareTo(stock.date);
		}

		return response;
	}

}
