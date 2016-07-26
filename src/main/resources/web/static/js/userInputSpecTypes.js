function addRow(tableID) {
  var table = document.getElementById(tableID);
  var rowCount = table.rows.length;
  var row = table.insertRow(rowCount);
  var cell0 = row.insertCell(0);
  var element0 = document.createElement("input");
  element0.type = "checkbox";
  cell0.appendChild(element0);
  var cell1 = row.insertCell(1);
  var element1 = document.createElement("input");
  element1.type = "text";
  cell1.appendChild(element1);
  var cell2 = row.insertCell(2);
  var element2 = document.createElement("input");
  element2.type = "text";
  cell3.appendChild(element2);
}
function deleteRow(tableID) {
  try {
          var table = document.getElementById(tableID);
          var rowCount = table.rows.length;
          for(var i=0; i<rowCount; i++) {
            var row = table.rows[i];
            var chkbox = row.cells[0].childNodes[0];
            if(null != chkbox && true == chkbox.checked) {
              table.deleteRow(i);
              rowCount--;
              i--;
            }
          }
      } catch(e) {
    alert(e);
  }
}
