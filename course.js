async function renderList() {
    let response = await fetch(`/api/course/list`);
    if (!response.ok) {
        console.error(response);
        return;
    }

    let data = await response.json();

    let tbodyEl = document.createElement("tbody");
    for (let item of data) {
        let trEl = document.createElement("tr");
        tbodyEl.append(trEl);

        let tdEl;
        tdEl = document.createElement("td");
        tdEl.innerText = item.cou_name;
        tdEl.className = "col-cou_name";
        trEl.append(tdEl);

        tdEl = document.createElement("td");
        tdEl.className = "col-cou_no";
        tdEl.innerText = item.cou_no;
        trEl.append(tdEl);

        tdEl = document.createElement("td");
        tdEl.className = "col-teacher";
        tdEl.innerText = item.teacher;
        trEl.append(tdEl);

        tdEl = document.createElement("td");
        tdEl.className = "col-term";
        tdEl.innerText = item.term;
        trEl.append(tdEl);

        tdEl = document.createElement("td");
        tdEl.className = "col-room";
        tdEl.innerText = item.room;
        trEl.append(tdEl);

        tdEl = document.createElement("td");
        tdEl.className = "col-week";
        tdEl.innerText = item.week;
        trEl.append(tdEl);

        tdEl = document.createElement("td");
        tdEl.className = "col-day";
        tdEl.innerText = item.day;
        trEl.append(tdEl);

        tdEl = document.createElement("td");
        tdEl.className = "col-jie";
        tdEl.innerText = item.jie;
        trEl.append(tdEl);

        tdEl = document.createElement("td");
        tdEl.className = "ctrlbar";
        tdEl.append(renderRecordCtrlbar(item));
        trEl.append(tdEl);
    }

    let tableEl = document.querySelector("#course-table");
    document.querySelector("#course-table > tbody").remove();
    tableEl.append(tbodyEl);
}

function renderRecordCtrlbar(item) {
    let ctrlbarEl = document.createElement("div");
//修改按钮
    let editBtn = document.createElement("a");
    editBtn.className = "btn";
    editBtn.innerText = "修改";
    editBtn.onclick = (e) => {
        openEditDialog(item);
    };
    ctrlbarEl.append(editBtn);
//删除按钮
    let delBtn = document.createElement("a");
    delBtn.className = "btn";
    delBtn.innerText = "删除";
    delBtn.onclick = (e) => {
        openComfirmationDialog({
            message: `确定要删除“${item.cou_name}(#${item.cou_sn})”的信息？`,
            onOk: () => {
                (async () => {
                    let response = await fetch(`/api/course/${item.cou_sn}`, {
                        method: "DELETE",
                    });

                    if (!response.ok) {
                        console.error(response);
                    }

                    renderList();
                })();
            },
        });
    };
    ctrlbarEl.append(delBtn);
//查询按钮
    let queryBtn = document.createElement("a");
    queryBtn.className = "btn";
    queryBtn.innerText = "查询";
    queryBtn.onclick = (e) => {
       openQueryDialog(items);
    };
    ctrlbarEl.append(queryBtn);

    return ctrlbarEl;
}

async function openEditDialog(item) {
    let dialog = document.querySelector(".course-editor");

    let dialogTitle = dialog.querySelector(".dialog-head");
    let form = dialog.querySelector("form");

    if (item) {
        dialogTitle.innerText = `修改课程信息 (#${item.cou_sn})`;
        form.elements.cou_sn.value = item.cou_sn ?? null;
        form.elements.cou_no.value = item.cou_no ?? "";
        form.elements.cou_name.value = item.cou_name ?? "";
        form.elements.teacher.value = item.teacher ?? "";
        form.elements.term.value = item.term ?? "";
        form.elements.room.value = item.room ??"";
        form.elements.week.value = item.week ??"";
        form.elements.day.value = item.day ??"";
        form.elements.jie.value = item.jie ??"";

    } else {
        dialogTitle.innerText = "新建课程信息";
        form.elements.cou_sn.value = null;
        form.elements.cou_no.value = "";
        form.elements.cou_name.value = "";
        form.elements.teacher.value = "";
        form.elements.term.value = "";
        form.elements.room.value = "";
        form.elements.week.value = "";
        form.elements.day.value = "";
        form.elements.jie.value = "";
    }

    if (dialog.classList.contains("open")) {
        dialog.classList.remove("open");
    } else {
        dialog.classList.add("open");
    }
}

async function renderEditDialog() {
    let newCourseBtn = document.querySelector(".paper #new-btn");
    newCourseBtn.onclick = (e) => {
        openEditDialog();
    };

    let dialog = document.querySelector(".course-editor");

    let form = dialog.querySelector("form");

    let close_btn = dialog.querySelector("#close-btn");

    let closeDialog = () => {
        dialog.classList.remove("open");
    };

    close_btn.onclick = closeDialog;

    let save_btn = dialog.querySelector("#save-btn");
    save_btn.onclick = (e) => {
        let data = {
            cou_sn: form.elements.cou_sn.value,
            cou_no: form.elements.cou_no.value,
            cou_name: form.elements.cou_name.value,
            teacher: form.elements.teacher.value,
            term: form.elements.term.value,
            room: form.elements.room.value,
            week: form.elements.week.value,
            day: form.elements.day.value,
            jie: form.elements.jie.value,
        };

        if (!data.cou_sn) {
            // 异步执行POST请求操作
            (async () => {
                let response = await fetch("/api/course", {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json;charset=utf-8",
                    },
                    body: JSON.stringify(data),
                });

                if (!response.ok) {
                    console.error(response);
                    return;
                }
                closeDialog();
                renderList();
            })();
        } else {
            // 异步执行PUT请求操作
            (async () => {
                let response = await fetch(`/api/course/${data.cou_sn}`, {
                    method: "PUT",
                    headers: {
                        "Content-Type": "application/json;charset=utf-8",
                    },
                    body: JSON.stringify(data),
                });

                if (!response.ok) {
                    console.error(response);
                    return;
                }
                closeDialog();
                renderList();
            })();
        }
    };
}

async function openComfirmationDialog({ message, onOk, onCancel }) {
    let dialog = document.querySelector(".comfirmation-dialog");

    let closeDialog = () => {
        dialog.classList.remove("open");
    };

    let okBtn = dialog.querySelector("#ok-btn");
    okBtn.onclick = (e) => {
        if (typeof onOk === "function") {
            onOk();
        }

        closeDialog();
    };

    let cancelBtn = dialog.querySelector("#cancel-btn");
    cancelBtn.onclick = (e) => {
        if (typeof onCancel === "function") {
            onCancel();
        }

        closeDialog();
    };

    let messageEl = dialog.querySelector("#message");
    messageEl.innerText = message;

    dialog.classList.add("open");
}

document.addEventListener("DOMContentLoaded", (e) => {
    renderList();
    renderEditDialog();
});
