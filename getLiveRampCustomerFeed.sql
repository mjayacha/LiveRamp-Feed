
		WITH marketplaceUsers AS (
			SELECT
				userDBID,
				email
			FROM omc.dbo.userdb WITH (NOLOCK)
			WHERE email LIKE ('%@marketplace.amazon.com')
				OR email LIKE ('%@marketplace.newegg.com')
				OR email LIKE ('%@seller.sears.com')
				OR email LIKE ('%@example.com')
		),
		maxOrder AS (
			SELECT
				o.userDBID,
				o.placedDate AS placedDate,
				o.orderDate AS orderDate,
				ROW_NUMBER() OVER (PARTITION BY o.userDBID  ORDER BY COALESCE(o.placedDate, o.orderDate) DESC) AS rn,
				o.siteName,
				CASE
				WHEN o.placedBy = 'web' THEN 'Web'
				ELSE 'Phone'
				END AS placedBy
			FROM omc.dbo.orders o WITH (NOLOCK)
			WHERE o.userDBID  IS NOT NULL
			AND NOT EXISTS (
				SELECT 1
				FROM omc.dbo.noChargeReplacement_log ncrl WITH (NOLOCK)
				WHERE o.orderNumber = ncrl.new_orderNumber
			)
			AND o.status NOT IN ('cancelled')
		)

		SELECT DISTINCT
			u.firstName AS firstName,
			u.lastName AS lastName,
			u.email AS email,
			u.billingAddress AS address1,
			u.billingAddress2 AS address2,
			u.billingCity AS city,
			u.billingState AS state,
			u.billingZip AS postalCode,
			u.phoneNumber AS phoneNumber,
			cast(isNull(p.subscribed,0) as bit) AS proStatus,
			p.proBusinessType AS proBusinessType,
			mo.siteName AS lastOrderSite,
			FORMAT(COALESCE(mo.placedDate, mo.orderDate), 'yyyyMMdd') AS lastOrderDate,
			mo.placedBy AS lastOrderPlacedBy,
			uo.oid AS OID,
			ps.PRO_SEGMENT AS proType,
			FORMAT(ps.PRO_ACCEPTANCE_DATE, 'yyyyMMdd') AS proAcceptanceDate
		FROM maxOrder mo
		INNER JOIN Departments.analyticsTeam.userOID uo
			ON uo.userDBID = mo.userDBID
		LEFT JOIN Departments.analyticsTeam.proSegments ps
			ON uo.oid = ps.OID
		LEFT JOIN omc.dbo.userDB u WITH (NOLOCK)
			ON mo.userDBID  = u.userDBID
		LEFT JOIN omc.dbo.vw_pro p (NOLOCK)
			ON p.CustomerID = mo.userDBID
		LEFT JOIN omc.dbo.userDB_shipping uds
			ON (mo.userDBID = uds.userDBId
			AND uds.Active = 'Y')
		WHERE mo.RN = 1
			AND mo.userDBID != 1
			AND COALESCE(mo.placedDate, mo.orderDate) BETWEEN @startDate AND @endDate
			AND NOT EXISTS(SELECT 1 FROM marketplaceUsers WHERE marketplaceUsers.userDBID  = u.userDBID)
