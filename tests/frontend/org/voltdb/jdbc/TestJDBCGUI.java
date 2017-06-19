package org.voltdb.jdbc;

import java.awt.*;
import java.awt.event.*;
import java.sql.*;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

/**
 * Class to demo and test the JDBC code via a GUI.
 */
public class TestJDBCGUI extends Frame {
	Connection con;
	TextArea queryText, resultText;
	JPanel p1, p2, p3;
	JButton b1, b2, b3, b4, b5, b6;

	static Statement stmt;
	
	public TestJDBCGUI() {
		JFrame frame = new JFrame("JDBC Demo");

		p1 = new JPanel();
		p2 = new JPanel();
		p3 = new JPanel();
		
		p1.setLayout(new BorderLayout());
		p2.setLayout(new GridLayout(2, 3, 15, 15));
		p3.setLayout(new BorderLayout());
		
		p1.setBorder(new EmptyBorder(10, 10, 10, 10));
		p2.setBorder(new EmptyBorder(20, 10, 10, 10));
		p3.setBorder(new EmptyBorder(10, 10, 10, 10));
		
		queryText = new TextArea("Enter query", 8, 60);
		resultText = new TextArea("Result", 5, 60);
		
		b3 = new JButton("Select");
		b4 = new JButton("Update");
		b5 = new JButton("Insert");
		b6 = new JButton("Delete");

		p2.add(b3);
		p2.add(b4);
		p2.add(b5);
		p2.add(b6);
		
		p1.add("Center", queryText);
		p1.add("South", p2);
		p3.add(resultText);
		
		frame.getContentPane().add(p1, BorderLayout.NORTH);
		frame.getContentPane().add(p3, BorderLayout.SOUTH);
		frame.pack();
		frame.setVisible(true);
		frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		b3.addActionListener(new Select());
		b5.addActionListener(new Insert());
		b4.addActionListener(new Update());
		b6.addActionListener(new Delete());
	
	} {
		 try {
		        String url ="jdbc:voltdb://localhost:21212";
		        Class.forName("org.voltdb.jdbc.Driver");
		        con=DriverManager.getConnection(url);
		  
		 } catch(Exception e){
			 e.getMessage();
		 }
	}

	class Select implements ActionListener {
		@Override
		public void actionPerformed(ActionEvent e) {
			try {
				stmt  = con.createStatement();
				String query = queryText.getText();
				ResultSet rs = stmt.executeQuery(query);
				int count = 0;
				
				int numCols = rs.getMetaData().getColumnCount();
				resultText.append("\n");
				for(int i = 1; i <= numCols; i++){
					resultText.append(rs.getMetaData().getColumnName(i) + " ");
				}
				
				while(rs.next()){
					resultText.append("\n");
					for(int i = 1; i <= numCols; i++){
						String type = rs.getMetaData().getColumnTypeName(i);

						if(type.equals("INTEGER")){
							resultText.append(rs.getInt(i)+ " ");
						} else  if(type.equals("VARCHAR")){
							resultText.append(rs.getString(i)+ " ");
						} else if (type.equals("TIMESTAMP")){
							resultText.append(rs.getTimestamp(i) + " ");
						} else if (type.equals("BOOLEAN")){
							resultText.append(rs.getBoolean(i) + " ");
						} else if (type.equals("DECIMAL") || type.equals("FLOAT") ) {
							resultText.append(rs.getDouble(i) + " ");
						} else if (type.equals("STRING")){
							resultText.append(rs.getString(i) + " ");
						}
						
					}
					count++;
				}
				
				resultText.append("\n" + count + " rows selected");
				rs.close();
				stmt.close();
			} catch (Exception ex) {
				resultText.setText("Error during select");
				ex.getMessage();
			}
		}
	}

	class Update implements ActionListener {
		@Override
		public void actionPerformed(ActionEvent e) {
			try {
				stmt  = con.createStatement();
				String query = queryText.getText();
				int rowCount = stmt.executeUpdate(query);

				if (rowCount == -1) {
					resultText.append("Table dropped");
				} else if (rowCount == 0) {
					resultText.append("Query did not complete. Check that the tuple exists.");
				} else {
					resultText.append("\n Success: " + rowCount + " rows updated");
				}
				
				con.commit();
				stmt.close();
			} catch (Exception ex) {
				resultText.setText("Error during update");
				ex.printStackTrace();
			}
		}
	}

	class Insert implements ActionListener {
		@Override
		public void actionPerformed(ActionEvent e) {
			try { 
				stmt = con.createStatement();
				String query = queryText.getText();
				int rowCount  = stmt.executeUpdate(query);
				
				if (rowCount == -1) {
					resultText.append("Table dropped");
				} else if (rowCount == 0 ){
					resultText.append("There was a conflict during insertion.");
				} else {
					resultText.append("\n Success: " + rowCount + " rows inserted");
				}
				con.commit();
				stmt.close();
			} catch (Exception ex) {
				resultText.setText("Error during insert");
				ex.printStackTrace();
			}
		}
	}

	class Delete implements ActionListener {
		@Override
		public void actionPerformed(ActionEvent e) {
			try {
				stmt = con.createStatement();
				String query = queryText.getText();
				int rowCount = stmt.executeUpdate(query);
				
				if (rowCount == -1) {
					resultText.append("Table dropped");
				} else if (rowCount == 0 ){
					resultText.append("This tuple does not exist in the table.");
				} else {
					resultText.append("\n Success: " + rowCount + " row deleted");
				}
				
				con.commit();
				stmt.close();
			} catch (Exception ex) {
				resultText.setText("Error during delete");
				ex.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		new TestJDBCGUI();
	}

}