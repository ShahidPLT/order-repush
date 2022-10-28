//process.env.AWS_PROFILE = "plt-staging"
process.env.AWS_REGION = "eu-west-1"

require('dotenv').config()
const aws = require("aws-sdk");
const fs = require("fs");
const axios = require('axios');
var _ = require('lodash');
const fg = require("fast-glob");
const { readFile, writeFile } = require('fs/promises');
const path = require("path");
const uuidv4 = require('uuid/v4');

(async () => {
    const entries = await fg(["./batch/*.json"], { dot: false })
    for (const file of entries) {
        const orderMeta = JSON.parse(await readFile(file, 'binary'));
        console.log(`Processing-ReOrder ${file} ${orderMeta.orderNumber}`);

        const orderResponse = await getOrder(orderMeta.orderNumber);
        if (!orderResponse.Order) {
            console.log(`Order Not Exist ${orderMeta.orderNumber}`);
            moveFileToFailed(file);
            continue;
        }

        const refund = await getRefundById(orderMeta.refundId);
        if (!refund) {
            console.log(`${orderMeta.orderNumber} RefundId ${orderMeta.refundId} Not Found`);
            moveFileToFailed(file);
            continue;
        }

        if (refund.IsProcessed == "Cancelled") {
            console.log(`${orderMeta.orderNumber} RefundId ${orderMeta.refundId} Refund Already Cancelled - Can not continue`);
            moveFileToFailed(file);
            continue;
        }

        if (refund.IsProcessed == "Completed") {
            console.log(`${orderMeta.orderNumber} RefundId ${orderMeta.refundId} Refund Already Refunded - Can not continue`);
            moveFileToAlreadyRefunded(file);
            continue;
        }

        const order = orderResponse.Order;

        if (!isSkusExist(order, orderMeta.skus)) {
            console.log(`${orderMeta.orderNumber} One of SKU does not exist in Order`);
            moveFileToFailed(file);
            continue;
        }

        const stocks = await getStock(orderMeta.skus);

        const availableSkus = getAvailableSkus(stocks);
        if (!availableSkus.length) {
            console.log(`COLs Stock not available ${orderMeta.orderNumber}`);
            moveFileToRejected(file);
            continue;
        }

        const allSkusAvailable = isAllSkusAvailable(orderMeta.skus, availableSkus);
        if (!allSkusAvailable) {
            console.log(`${orderMeta.orderNumber} All COLs Stock not available `);
            moveFileToRejected(file);
            continue;
        }

        await cancelPendingFinanceApproval(order, refund);

        const orderItemsSkus = availableSkus.map(item => item.sku);
        const orderCreateResponse = await createOrder(mapReOrderRequest(order, orderItemsSkus));

        if (!orderCreateResponse.OrderNumber) {
            console.log(`Failed to Create ReOrder ${orderMeta.OrderNumber}`);
            moveFileToFailed(file);
            continue;
        }

        await insertLog(orderMeta.orderNumber, `COLs from Order to Re-Order, Re-Order Order Number ${orderCreateResponse.OrderNumber}`);
        await writeFile("reorder.csv", `${orderMeta.orderNumber},${orderCreateResponse.OrderNumber}, "${orderItemsSkus.join(",")}"`);
        await moveFileToDone(file);
    }
})();

function isAllSkusAvailable(colSkus, availableSkus) {
    countAvilableSkus = availableSkus.filter(sku => sku.stock > 0).length;

    return countAvilableSkus === colSkus.length
}

async function updateOrderLineStatusFinanceCancelled(order, refund) {
    const documentClient = new aws.DynamoDB.DocumentClient();
    const orderLines = order.Items;
    const updateOrderLineStatus = [];

    for (let refundItem of refund.RefundingLines) {
        const orderIndex = orderLines.findIndex(orderItem => orderItem.Sku.trim() === refundItem.ProductSku.trim());
        if (orderIndex < 0) {
            continue;
        }

        updateOrderLineStatus.push({
                Sku: refundItem.ProductSku.trim(),
                Status: 'Finance Cancelled',
                Type: 'Cancelled',
                Source: refund.Source,
                Qty: refundItem.Quantity,
            })
    }

    for(const status of updateOrderLineStatus) {
        const hash = uuidv4().substring(0, 5);
        const createdAt = new Date();

        const params = {
            TableName: "OrdersV3",
            Item: {
                AttributeId: `OrderLine#Status#${status.Sku}#${createdAt.getTime()}#${hash}`,
                OrderId: order.OrderNumber,
                CreatedAt: createdAt.toISOString(),
                Qty: parseInt(status.Qty, 10),
                Status: status.Status,
                Source: status.Source,
                Type: status.Type,
            }
        }

        await documentClient.put(params).promise();
    }
}


async function updateRefundCancelledApproval(itemId) {
    const documentClient = new aws.DynamoDB.DocumentClient();
    let params = {
        TableName: "Refunds",
        Key: {
            Id: itemId,
        },
        UpdateExpression: "set RefundApprove = :RefundApprove, IsProcessed=:IsProcessed, RefundApproveDate=:RefundApproveDate, IsException=:IsException",
        ExpressionAttributeValues: {
            ":IsException": "Cancelled by Finance",
            ":RefundApprove": "Cancelled",
            ":IsProcessed": "Cancelled",
            ":RefundApproveDate": Date.now()
        },
        ConditionExpression: "RefundApprove <> :RefundApprove",
        ReturnValues:"ALL_NEW"
    };

    return documentClient.update(params).promise();

}

async function cancelPendingFinanceApproval(order, refund) {
    await updateRefundCancelledApproval(refund.Id);

    if (refund.IsRefundingShipping) {
        if (!order.ShippingRefunded || order.ShippingRefunded !== "Yes") {
            await updateOrderShippingRefundedStatus("Finance Cancelled", order.OrderNumber)
        }
    }

    await updateOrderLineStatusFinanceCancelled(order, refund);
}

async function updateOrderShippingRefundedStatus(status, orderNumber) {
    const documentClient = new aws.DynamoDB.DocumentClient();
    const params = {
        TableName: "OrdersV3",
        Key: {
            OrderId: orderNumber,
            AttributeId: 'Details'
        },
        UpdateExpression: "SET #ShippingRefunded = :value",
        ExpressionAttributeValues: {
            ":value": status
        },
        ExpressionAttributeNames: {
            "#ShippingRefunded": "ShippingRefunded"
        }
    };

    return documentClient.update(params).promise();    
}

async function getRefundById(id) {
    const documentClient = new aws.DynamoDB.DocumentClient();
    const params = {
        TableName: "Refunds",
        KeyConditionExpression: "Id = :id",
        ExpressionAttributeValues: {
            ":id": id
        }
    };

    let response;
    try {
        response = await documentClient.query(params).promise();
    } catch (err) {
        throw err;
    }

    if (response.Count < 1) {
        return false;
    }

    return response.Items[0];
}


async function moveFileToRejected(file) {
    await fs.promises.rename(file, `./batch/rejected/${path.parse(file).base}`);
}

async function moveFileToDone(file) {
    await fs.promises.rename(file, `./batch/done/${path.parse(file).base}`);
}

async function moveFileToFailed(file) {
    await fs.promises.rename(file, `./batch/failed/${path.parse(file).base}`);
}

async function moveFileToAlreadyRefunded(file) {
    await fs.promises.rename(file, `./batch/already-refunded/${path.parse(file).base}`);
}

function mapReOrderRequest(orderDetail, skus) {
    const orderItems = getItems(orderDetail, skus)

    return {
        "StoreId": orderDetail.StoreId,
        "ParentOrderId": orderDetail.OrderId,
        "ParentOrderNumber": orderDetail.OrderNumber,
        "CurrencyCode": orderDetail.CurrencyCode,
        "DiscountCode": "RE-ORDER",
        "DiscountCouponDescription": "RE-ORDER",
        "BillingDetails": {
            "Address": {
                "City": orderDetail.BillingDetails.Address.City,
                "CountryCode": orderDetail.BillingDetails.Address.CountryCode,
                "Postcode": orderDetail.BillingDetails.Address.Postcode,
                "Region": orderDetail.BillingDetails.Address.Region,
                "Street": orderDetail.BillingDetails.Address.Street,
            },
            "FirstName": orderDetail.FirstName,
            "LastName": orderDetail.LastName,
        },
        "CustomerDetails": {
            "CustomerId": orderDetail.CustomerId,
            "Email": orderDetail.Email,
            "FirstName": orderDetail.FirstName,
            "LastName": orderDetail.LastName,
            "Phone": orderDetail.CustomerDetails.Phone,
        },
        "Items": orderItems,
        "ShippingDetails": {
            "Address": {
                "City": orderDetail.ShippingDetails.Address.City,
                "CountryCode": orderDetail.ShippingDetails.Address.CountryCode,
                "Postcode": orderDetail.ShippingDetails.Address.Postcode,
                "Region": orderDetail.ShippingDetails.Address.Region,
                "Street": orderDetail.ShippingDetails.Address.Street,
            },
            "FirstName": orderDetail.ShippingDetails.FirstName,
            "LastName": orderDetail.ShippingDetails.LastName,
            "BasePrice": parseFloat(orderDetail.ShippingDetails.Price),
            "Price": parseFloat(orderDetail.ShippingDetails.Price),
            "Method": orderDetail.ShippingDetails.Method,
            "Type": orderDetail.ShippingDetails.Type,
        }
    }
}

async function createOrder(data) {
    let url = `${process.env.OMS_ENDPOINT}reorder`;
    const config = {
        'headers': {
            'Content-Type': 'application/json',
            'x-api-key': process.env.OMS_API_KEY
        }
    };

    try {
        const response = await axios.put(url, data, config);

        return response.data;
    } catch (err) {
        if (err.response && err.response.data) {
            console.log(err.response.data);
            throw new Error(err.response.data);
        }

        console.error(err.message);

        throw new Error(err);
    }
}

function isSkusExist(order, skus) {
    return skus.every(sku => order.Items.find(item => item.Sku == sku));
}

async function getOrder(orderNumber) {
    let url = `${process.env.OMS_ENDPOINT}${orderNumber}`;
    const config = {
        'headers': {
            'Content-Type': 'application/json',
            'x-api-key': process.env.OMS_API_KEY
        }
    };

    try {
        const response = await axios.get(url, config);

        return response.data;
    } catch (err) {
        if (err.response && err.response.data) {
            console.log(err.response.data);
            throw new Error(err.response.data);
        }

        console.log(err.message);

        throw new Error(err);
    }
}

function getItems(order, skus) {
    return order.Items.filter(item => skus.includes(item.Sku))
        .map(item => {
            return {
                Discount: 0,
                Sku: item.Sku,
                Quantity: parseFloat(item.Quantity),
                Image: item.Image,
                Name: item.Name,
                ProductType: item.ProductType,
                Size: item.Size,
                OriginalPrice: parseFloat(item.OriginalPrice),
                Price: parseFloat(item.Price),
                SelectedValues: JSON.parse(item.ProductOptions).attributes_info,
            }
        });
}

async function getStock(skus) {
    const encodeSkus = encodeURIComponent(skus);
    let url = `${process.env.IS_ENDPOINT}stock?skus=[${encodeSkus}]`;
    const config = {
        'headers': {
            'Content-Type': 'application/json',
            'x-api-key': process.env.IS_API_KEY
        }
    };

    try {
        const response = await axios.get(url, config);

        return response.data;
    } catch (err) {
        if (err.response && err.response.data) {
            console.error(err.response);
            throw new Error(err.response.data);
        }

        throw new Error(err);
    }
}

function getAvailableSkus(stocks) {
    return stocks.skus.map(sku => {
        const stockSku = Object.keys(sku)[0];
        const stockData = sku[stockSku];
        const jdaStock = stockData['GB-SHE-JDA-1'] || 0;
        const wamwasStock = stockData['GB-SHE-WAMAS-1'] || 0;

        if (stockData.total <= 0) {
            return {
                sku: stockSku,
                stock: stockData.total
            }
        }

        if (wamwasStock > 0) {
            return {
                allocation: 'GB-SHE-WAMAS-1',
                sku: stockSku,
                stock: wamwasStock
            }
        }

        if (jdaStock > 0) {
            return {
                allocation: 'GB-SHE-JDA-1',
                sku: stockSku,
                stock: jdaStock
            }
        }
    }).filter(Boolean);
}

async function insertLog(orderNumber, comment) {
    const documentClient = new aws.DynamoDB.DocumentClient();
    const params = {
        TableName: "OrdersLogs",
        Item: {
            Id: uuidv4(),
            OrderId: orderNumber,
            CreatedAt: new Date().toISOString(),
            User: 'System',
            Type: 'Shipment',
            Comment: comment,
            UserId: null,
            LogData: {},
        },
    }

    await documentClient.put(params).promise();
}